class Handler
# encoding: utf-8
  require "logstash/inputs/base"
  require "logstash/inputs/s3/remote_file"

  module LogStash module Inputs class S3SNSSQS < LogStash::Inputs::Base
    class Handler

    end
      s3bucket[bucket] = @s3_resource.bucket(bucket)
    end




    def get_s3client(bucket_name)
      ##FIX_ME: move hash merge to main class?!
      @s3_clients[bucket_name] ||= Aws::S3::Client.new(aws_options_hash.merge(s3_options_by_bucket[bucket_name])
      return @s3_clients[bucket_name]
    end

    private
    def get_s3object
      #Geht das auch ohne Resource???
      #Geht das auch ohne Resource???
      s3 = Aws::S3::Resource.new(client: @s3_client)
    end

    private
    def s3_assume_role()
      Aws::AssumeRoleCredentials.new(
          client: Aws::STS::Client.new(region: @region),
          role_arn: @s3_role_arn,
          role_session_name: @s3_role_session_name
      )
    end