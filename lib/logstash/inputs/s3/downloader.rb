# encoding: utf-8
require 'fileutils'
require 'thread'
require "logstash/inputs/base"
#require "logstash/inputs/s3/remote_file"

module LogStash module Inputs class S3SNSSQS < LogStash::Inputs::Base
  class S3Downloader

    def initialize(options)
      @tempdir = options[:temporary_directory]
      @factory = options[:s3_client_factory]
    end

    def copy_s3object_to_disk(record)
      completed = false
      # (from docs) WARNING:
      # yielding data to a block disables retries of networking errors!
      File.open(record[:local_file], 'wb') do |file|
        begin
          @factory.get_s3_client(record[:bucket]) do |s3|
            s3.get_object(bucket: record[:bucket], key: record[:key]) do |chunk|
              return completed if stop?
              file.write(chunk)
            end
          end
          # determine codec:
          record[:codec] = ...
        rescue Aws::S3::Errors::ServiceError => e
          @logger.error("Unable to download file. Requeuing the message", :record => record)
          # prevent sqs message deletion
          throw :skip_delete
        end
      end
      completed = true
      return completed
    end

    def delete_s3_object(record)
      @factory.get_s3_client(record[:bucket]) do |s3|
        s3.delete_object(bucket: record[:bucket], key: record[:key])
      end
    end
