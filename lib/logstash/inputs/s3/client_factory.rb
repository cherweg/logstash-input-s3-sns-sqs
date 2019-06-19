# not needed - Mutex is part of core lib:
#require 'thread'

class S3ClientFactory

  def initialize(logger, options, aws_options_hash)
    @logger = logger
    @aws_options_hash = aws_options_hash
    @s3_default_options = Hash[options[:s3_default_options].map { |k, v| [k.to_sym, v] }]
    @aws_options_hash.merge!(@s3_default_options) unless @s3_default_options.empty?
    @sts_client = Aws::STS::Client.new(region: options[:aws_region])
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
        options = @aws_options_hash.clone
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
    #@mutexes_by_bucket[bucket_symbol].synchronize do
    # So we are testing this without this mutex.
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
