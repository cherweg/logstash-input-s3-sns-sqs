# not needed - Mutex is part of core lib:
#require 'thread'

module LogStash module Inputs class S3SNSSQS < LogStash::Inputs::Threadable
  class S3ClientFactory

    def initialize(options)
      # FIXME: region per bucket?
      @sts_client = Aws::STS::Client.new(region: options[:aws_region])
      # FIXME: options are non-generic (...by_bucket mixes credentials with folder stuff)
      @credentials_by_bucket = options[:s3_credentials_by_bucket]
      # lazy-init this as well:
      # @credentials_by_bucket = @options_by_bucket.map { |bucket, options|
      #   [bucket.to_sym, assume_s3_role(options['credentials'])]
      # }.to_h
      @default_session_name = options[:s3_role_session_name]
      @clients_by_bucket = {}
      #@mutexes_by_bucket = {}
      @creation_mutex = Mutex.new
    end

    def get_s3client(bucket_name)
      bucket_symbol = bucket_name.to_sym
      @creation_mutex.synchronize do
        if @clients_by_bucket[bucket_symbol].nil?
          options = aws_options_hash
          if @credentials_by_bucket[bucket_name]
            options.merge!(credentials: assume_s3_role(@credentials_by_bucket[bucket_name]))
          end
          @clients_by_bucket[bucket_symbol] = Aws::S3::Client.new(options)
          #@mutexes_by_bucket[bucket_symbol] = Mutex.new
        end
      end
      # to be thread-safe, one uses this method like this:
      # s3_client_factory.get_s3client(my_s3_bucket) do
      #   ... do stuff ...
      # end
      # FIXME: this does not allow concurrent downloads from the same bucket!
      # So we are testing this without this mutex.
      #@mutexes_by_bucket[bucket_symbol].synchronize do
      yield @clients_by_bucket[bucket_name]
      #end
    end

    private

    def assume_s3_role(credentials)
      # reminder: these are auto-refreshing!
      return Aws::AssumeRoleCredentials.new(
          client: @sts_client,
          role_arn: credentials['role'],
          role_session_name: @s3_role_session_name
      ) if credentials['role']
    end

  end # class
end;end;end
