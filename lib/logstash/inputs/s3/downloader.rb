# encoding: utf-8
require 'fileutils'
require 'thread'
require "logstash/inputs/base"
#require "logstash/inputs/s3/remote_file"

module LogStash module Inputs class S3SNSSQS < LogStash::Inputs::Base
  class S3Downloader

    def initialize(options)
      @tempdir = options[:temporary_directory]
      @options_by_bucket = options[:s3_options_by_bucket].map do |bucket, options|
      @clients_by_bucket = {}
    end

    def get_s3client(bucket_name)
      if clients_by_bucket[bucket_name].nil?
        options = aws_options_hash
        options.merge!(@options_by_bucket[bucket_name]) if @options_by_bucket[bucket_name]
        @clients_by_bucket[bucket_name] = Aws::S3::Client.new(options)
      end
      return @clients_by_bucket[bucket_name]
    end

    private
    # -^- from here on... -^-

    def copy_s3object_to_disk(bucket_name, key, local_file)
      #filename = File.join(@tempdir, File.basename(key))
      s3 = get_s3_client(bucket_name)
      completed = false
      # WARNING: yielding data to a block disables retries of networking errors!
      File.open(local_file, 'wb') do |file|
        begin
          s3.get_object(bucket: bucket_name, key: key) do |chunk|
            return completed if stop?
            file.write(chunk)
          end
        rescue Aws::S3::Errors::ServiceError => e
          @logger.error("Unable to download file. Requeuing the message", :file => remote_object.inspect)
          # prevent sqs message deletion
          throw :skip_delete
        end
      end
      completed = true
      return completed
    end

    def assume_s3_role()
      Aws::AssumeRoleCredentials.new(
          client: Aws::STS::Client.new(region: @region),
          role_arn: @s3_role_arn,
          role_session_name: @s3_role_session_name
      )
    end
