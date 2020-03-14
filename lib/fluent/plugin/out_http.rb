class Fluent::HTTPOutput < Fluent::Output
  Fluent::Plugin.register_output('http', self)

  class RecoverableResponse < StandardError; end

  def initialize
    super
    require 'json'
    require 'net/https'
    require 'uri'
    require 'yajl'
  end

  # Endpoint URL ex. localhost.local/api/
  config_param :endpoint_url, :string

  # Set Net::HTTP.verify_mode to `OpenSSL::SSL::VERIFY_NONE`
  config_param :ssl_no_verify, :bool, :default => false

  # HTTP method
  config_param :http_method, :string, :default => :post

  # form | json
  config_param :serializer, :string, :default => :form

  # Simple rate limiting: ignore any records within `rate_limit_msec`
  # since the last one.
  config_param :rate_limit_msec, :integer, :default => 0

  # Raise errors that were rescued during HTTP requests?
  config_param :raise_on_error, :bool, :default => true

  # Specify recoverable error codes
  config_param :recoverable_status_codes, :array, value_type: :integer, default: [503]

  # custom headers
  config_param :custom_headers, :string, :default => '{}'

  # nil | 'none' | 'basic'
  config_param :authentication, :string, :default => nil
  config_param :username, :string, :default => ''
  config_param :password, :string, :default => '', :secret => true

  config_param :bulk_request, :bool, :default => false

  def configure(conf)
    super

    @ssl_verify_mode = if @ssl_no_verify
                         OpenSSL::SSL::VERIFY_NONE
                       else
                         OpenSSL::SSL::VERIFY_PEER
                       end

    serializers = [:json, :form]
    @serializer = if serializers.include? @serializer.intern
                    @serializer.intern
                  else
                    :form
                  end

    http_methods = [:get, :put, :post, :delete]
    @http_method = if http_methods.include? @http_method.intern
                     @http_method.intern
                   else
                     :post
                   end

    @auth = case @authentication
            when 'basic' then :basic
            else
              :none
            end

    @last_request_time = nil

    if @bulk_request
      @serializer = :x_ndjson # secret settings for bulk_request
    end
  end

  def start
    super
  end

  def shutdown
    super
  end

  def format_url(tag, time, record)
    @endpoint_url
  end

  def set_body(req, tag, time, record)
    if @serializer == :json
      set_json_body(req, time, record)
    elsif @serializer == :x_ndjson
      set_bulk_body(req, record)
    else
      req.set_form_data(record)
    end
    req
  end

  def set_header(req, tag, time, record)
    JSON.parse(@custom_headers).each do |k,v|
      req[k] = v
    end
    req
  end

  def set_json_body(req, time, data)
    if data['time'].nil? && time.is_a?(Integer)
      data['time'] = Time.at(time).utc.to_datetime.rfc3339
    end
    req.body = Yajl.dump(data)
    req['Content-Type'] = 'application/json'
  end

  def set_bulk_body(req, data)
    arr = []
    data.each do |time, record|
      if record['time'].nil? && time.is_a?(Integer)
        record['time'] = Time.at(time).utc.to_datetime.rfc3339
      end
      arr.push(Yajl.dump(record))
    end
    req.body = arr.join("\n")
    req['Content-Type'] = 'application/x-ndjson'
  end

  def create_request(tag, time, record)
    url = format_url(tag, time, record)
    uri = URI.parse(url)
    req = Net::HTTP.const_get(@http_method.to_s.capitalize).new(uri.path)
    set_body(req, tag, time, record)
    set_header(req, tag, time, record)
    return req, uri
  end

  def http_opts(uri)
    opts = {
        :use_ssl => uri.scheme == 'https'
    }
    opts[:verify_mode] = @ssl_verify_mode if opts[:use_ssl]
    opts
  end

  def send_request(req, uri)
    is_rate_limited = (@rate_limit_msec != 0 and not @last_request_time.nil?)
    if is_rate_limited and ((Time.now.to_f - @last_request_time) * 1000.0 < @rate_limit_msec)
      $log.info('Dropped request due to rate limiting')
      return
    end

    res = nil

    begin
      if @auth and @auth == :basic
        req.basic_auth(@username, @password)
      end
      @last_request_time = Time.now.to_f
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = uri.scheme == 'https'
      http.verify_mode = @ssl_verify_mode
      res = http.start {|http| http.request(req) }
    rescue => e # rescue all StandardErrors
      # server didn't respond
      $log.warn "Net::HTTP.#{req.method.capitalize} raises exception: #{e.class}, '#{e.message}'"
      raise e if @raise_on_error
    else
      unless res and res.is_a?(Net::HTTPSuccess)
        res_summary = if res
                        "#{res.code} #{res.message} #{res.body}"
                      else
                        "res=nil"
                      end
        if @recoverable_status_codes.include?(res.code.to_i)
          raise RecoverableResponse, res_summary
        else
          $log.warn "failed to #{req.method} #{uri} (#{res_summary})"
        end
      end #end unless
    end # end begin
  end # end send_request

  def handle_record(tag, time, record)
    req, uri = create_request(tag, time, record)
    send_request(req, uri)
  end

  def handle_records(tag, time, es)
    req, uri = create_request(tag, time, es)
    send_request(req, uri)
  end

  def emit(tag, es, chain)
    if @bulk_request
      time = Fluent::Engine.now
      handle_records(tag, time, es)
    else
      es.each do |time, record|
        handle_record(tag, time, record)
      end
    end
    chain.next
  end
end
