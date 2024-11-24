require 'fluent/plugin/input'
require 'aws-sdk-s3'

module Fluent::Plugin
  class S3NoindexInput < Input
    Fluent::Plugin.register_input('s3_noindex', self)

    helpers :timer

    config_param :s3_bucket, :string, default: nil
    config_param :region, :string, default: nil
    config_param :download_directory, :string, default: '/tmp/fluent-plugin-s3'
    config_param :timestamp_directory, :string, default: '/tmp/fluent-plugin-s3-timestamps'
    config_param :tag, :string
    config_param :interval, :time, default: 60
    config_param :delete_local_files, :bool, default: true

    def configure(conf)
      super

      @s3 = Aws::S3::Client.new(region: @region)
      Dir.mkdir(@download_directory) unless Dir.exist?(@download_directory)
      Dir.mkdir(@timestamp_directory) unless Dir.exist?(@timestamp_directory)
      @timestamp_file = "#{@timestamp_directory}/last_modified_timestamp.txt"
      @last_modified_timestamp = load_timestamp
    end

    def start
      super
      timer_execute(:s3_input, @interval, &method(:poll))
    end

    def shutdown
      super
    end

    private

    def poll
      objects = list_objects
      download_objects(objects)
      delete_local_files_not_in_s3(objects) if @delete_local_files
      save_timestamp
    end

    def list_objects
      @s3.list_objects_v2(bucket: @s3_bucket).contents
    end

    def download_objects(objects)
      objects.each do |object|
        file_path = File.join(@download_directory, object.key)

        if File.exist?(file_path) && File.size(file_path) == object.size
          log.info "Skipping #{object.key} (already downloaded)"
          next
        end

        log.info "Downloading #{object.key} to #{file_path}"
        @s3.get_object(bucket: @s3_bucket, key: object.key) do |chunk|
          File.open(file_path, 'a+b') { |f| f.write(chunk) }
        end
        log.info "Downloaded #{object.key}"

        @last_modified_timestamp = object.last_modified

        # Emit the downloaded file content to Fluentd in Uncompressed format
        begin
          file_content = file_content = Zlib::GzipReader.open(file_path) { |gz| gz.read }
          router.emit(@tag, Fluent::Engine.now, file_content)
        rescue => e
          log.error "Error reading or emitting file: #{e.message}"
        end
      end
    end

    def delete_local_files_not_in_s3(objects)
      local_files = Dir.glob("#{@download_directory}/*")
      local_files.each do |file|
        file_name = File.basename(file)
        unless objects.any? { |object| object.key == file_name }
          log.info "Deleting #{file}"
          File.delete(file)
        end
      end
    end

    def load_timestamp
      if File.exist?(@timestamp_file)
        Time.parse(File.read(@timestamp_file))
      else
        Time.at(0)
      end
    end

    def save_timestamp
      File.write(@timestamp_file, @last_modified_timestamp.to_s)
    end
  end
end