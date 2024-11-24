require 'fluent/plugin/output'
require 'zlib'
require 'json'
require 'net/http'
require 'uri'

module Fluent::Plugin
  class HttpNdjsonOutput < Output
    Fluent::Plugin.register_output('http_ndjson', self)

    helpers :compat_parameters

    desc 'The URL of the HTTP endpoint'
    config_param :endpoint_url, :string

    def configure(conf)
      compat_parameters_convert(conf, :buffer)
      super
      @uri = URI.parse(@endpoint_url)
    end

    def process(tag, es)
      begin
        # Assuming the input is a single gzipped NDJSON file
        ndjson_data = es.first[1].to_s  # Get the data from the first event

        # Split the NDJSON data into individual JSON objects
        json_objects = []
        ndjson_data.split("\n").each do |line|
          begin
            json_objects << JSON.parse(line)
          rescue JSON::ParserError => e
            log.warn "Skipping invalid JSON line: #{line.inspect} - #{e.message}"
          end
        end

        # Wrap the JSON objects in an 'events' array
        payload = { 'events' => json_objects }

        # Log the JSON payload before sending
        log.debug "Sending JSON payload: #{payload.to_json}"

        # Prepare the HTTP request
        http = Net::HTTP.new(@uri.host, @uri.port)
        request = Net::HTTP::Post.new(@uri.request_uri, 'Content-Type' => 'application/json')
        request.body = payload.to_json

        # Send the request
        response = http.request(request)

        # Handle the response (optional)
        log.info "Response code: #{response.code}"
        log.debug "Response body: #{response.body}"

      rescue StandardError => e
        log.error "Error sending data: #{e.message}"
      end
    end
  end
end