# not needed - Mutex is part of core lib:
#require 'thread'
require "logstash/inputs/threadable"


#module LogStash module Inputs class S3SNSSQS < LogStash::Inputs::Threadable
class S3ClientFactory

  def initialize(logger, options, aws_options_hash)
    @logger = logger
    @aws_options_hash = aws_options_hash
    @s3_client_options: options[:s3_client_options]
    # FIXME: region per bucket?
    @sts_client = Aws::STS::Client.new(region: options[:aws_region])
    # FIXME: options are non-generic (...by_bucket mixes credentials with folder stuff)
    @credentials_by_bucket = options[:s3_credentials_by_bucket]
    @logger.debug("Credentials by Bucket", :credentials => @credentials_by_bucket)
    @default_session_name = options[:s3_role_session_name]
    @clients_by_bucket = {}
    #@mutexes_by_bucket = {}
    @creation_mutex = Mutex.new
  end

  def get_s3_client(bucket_name)
    bucket_symbol = bucket_name.to_sym
    @creation_mutex.synchronize do

      if @clients_by_bucket[bucket_symbol].nil?
        options = @aws_options_hash
        options.merge!(@s3_client_options) unless @s3_client_options.empty?
        unless @credentials_by_bucket[bucket_name].nil?
          options.merge!(credentials: get_s3_auth(@credentials_by_bucket[bucket_name]))
        end
        @clients_by_bucket[bucket_symbol] = Aws::S3::Client.new(options)
        @logger.debug("Created a new S3 Client", :bucket_name => bucket_name, :client => @clients_by_bucket[bucket_symbol], :used_options => options)
        #@mutexes_by_bucket[bucket_symbol] = Mutex.new
      end
    end
    # to be thread-safe, one uses this method like this:
    # s3_client_factory.get_s3_client(my_s3_bucket) do
    #   ... do stuff ...
    # end
    # FIXME: this does not allow concurrent downloads from the same bucket!
    # So we are testing this without this mutex.
    #@mutexes_by_bucket[bucket_symbol].synchronize do
    yield @clients_by_bucket[bucket_symbol]
    #end
  end

  private

  def get_s3_auth(credentials)
    # reminder: these are auto-refreshing!
    if credentials.key?('role')
      @logger.debug("Assume Role", :role => credentials["role"])
      return Aws::AssumeRoleCredentials.new(
        client: @sts_client,
        role_arn: credentials['role'],
        role_session_name: @default_session_name
      )
    elsif credentials.key?('access_key_id') && credentials.key?('secret_access_key')
      @logger.debug("Fetch credentials", :access_key => credentials['access_key_id'])
      return Aws::Credentials.new(credentials)
    end
  end

end # class
#end;end;end
