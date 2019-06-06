# encoding: utf-8
require 'fileutils'
require 'thread'
require "logstash/inputs/base"
#require "logstash/inputs/s3/remote_file"

module LogStash module Inputs class S3SNSSQS < LogStash::Inputs::Threadable
  class S3Downloader

    def initialize(options)
      @tempdir = options[:temporary_directory]
      @factory = options[:s3_client_factory]
      @delete_on_success = options[:delete_on_success]
    end

    def copy_s3object_to_disk(record)
      # (from docs) WARNING:
      # yielding data to a block disables retries of networking errors!
      File.open(record[:local_file], 'wb') do |file|
        return false if stop?
        begin
          @factory.get_s3_client(record[:bucket]) do |s3|
            response = s3.get_object(
              bucket: record[:bucket],
              key: record[:key],
              target: file)
          end
        rescue Aws::S3::Errors::ServiceError => e
          @logger.error("Unable to download file. Requeuing the message", :record => record)
          # prevent sqs message deletion
          throw :skip_delete
        end
      end
      return true
    end

    def cleanup_local_object(record)
      FileUtils.remove_entry_secure(record[:local_file], true) if File.exists?(record[:local_file])
    rescue Exception => e
      @logger.warn("Could not delete file", :file => record[:local_file], :error => e)
    end

    def cleanup_s3object(record)
      return unless @delete_on_success
      begin
        @factory.get_s3_client(record[:bucket]) do |s3|
          s3.delete_object(bucket: record[:bucket], key: record[:key])
        end
      rescue Exception => e
        @logger.warn("Failed to delete s3 object", :record => record, :error => e)
      end
    end

  end # class
end;end;end
