# encoding: utf-8
require "logstash/plugin_mixins/aws_config/generic"

module LogStash::PluginMixins::AwsConfig::V2
  def self.included(base)
    base.extend(self)
    base.send(:include, LogStash::PluginMixins::AwsConfig::Generic)
  end

  public
  def aws_options_hash
    opts = {}

    opts[:http_proxy] = @proxy_uri if @proxy_uri

    if @role_arn
      credentials = assume_role(opts.dup)
      opts[:credentials] = credentials
    else
      credentials = aws_credentials
      opts[:credentials] = credentials if credentials
    end

    if self.respond_to?(:aws_service_endpoint)
      # used by CloudWatch to basically do the same as bellow (returns { region: region })
      opts.merge!(self.aws_service_endpoint(@region))
    else
      # NOTE: setting :region works with the aws sdk (resolves correct endpoint)
      opts[:region] = @region
    end

    opts[:endpoint] = @endpoint unless @endpoint.nil?

    if respond_to?(:additional_settings)
      opts = symbolize_keys_and_cast_true_false(additional_settings).merge(opts)
    end

    if @use_aws_bundled_ca
      aws_core_library = Gem.loaded_specs['aws-sdk-core']&.full_gem_path or fail("AWS Core library not available")
      opts[:ssl_ca_bundle] = File.expand_path('ca-bundle.crt', aws_core_library).tap do |aws_core_ca_bundle|
        fail("AWS Core CA bundle not found") unless File.exists?(aws_core_ca_bundle)
      end
    end

    return opts
  end

  private

  def aws_credentials
    if @access_key_id && @secret_access_key
      Aws::Credentials.new(@access_key_id, @secret_access_key.value, @session_token ? @session_token.value : nil)
    elsif @access_key_id.nil? ^ @secret_access_key.nil?
      @logger.warn("Likely config error: Only one of access_key_id or secret_access_key was provided but not both.")
      secret_access_key = @secret_access_key ? @secret_access_key.value : nil
      Aws::Credentials.new(@access_key_id, secret_access_key, @session_token ? @session_token.value : nil)
    elsif @aws_credentials_file
      credentials_opts = YAML.load_file(@aws_credentials_file)
      credentials_opts.default_proc = lambda { |hash, key| hash.fetch(key.to_s, nil) }
      Aws::Credentials.new(credentials_opts[:access_key_id],
                           credentials_opts[:secret_access_key],
                           credentials_opts[:session_token])
    else
      nil # AWS client will read ENV or ~/.aws/credentials
    end
  end
  alias credentials aws_credentials

  def assume_role(opts = {})
    unless opts.key?(:credentials)
      credentials = aws_credentials
      opts[:credentials] = credentials if credentials
    end

    # for a regional endpoint :region is always required by AWS
    opts[:region] = @region

    Aws::AssumeRoleCredentials.new(
        :client => Aws::STS::Client.new(opts),
        :role_arn => @role_arn,
        :role_session_name => @role_session_name
    )
  end

  def symbolize_keys_and_cast_true_false(hash)
    case hash
    when Hash
      symbolized = {}
      hash.each { |key, value| symbolized[key.to_sym] = symbolize_keys_and_cast_true_false(value) }
      symbolized
    when 'true'
      true
    when 'false'
      false
    else
      hash
    end
  end

end