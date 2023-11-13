module LogStash::PluginMixins::AwsConfig::Generic
  def self.included(base)
    base.extend(self)
    base.generic_aws_config
  end

  def generic_aws_config
    # The AWS Region
    config :region, :validate => :string, :default => LogStash::PluginMixins::AwsConfig::US_EAST_1

    # This plugin uses the AWS SDK and supports several ways to get credentials, which will be tried in this order:
    #
    # 1. Static configuration, using `access_key_id` and `secret_access_key` params in the logstash plugin config
    # 2. External credentials file specified by `aws_credentials_file`
    # 3. Environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
    # 4. Environment variables `AMAZON_ACCESS_KEY_ID` and `AMAZON_SECRET_ACCESS_KEY`
    # 5. IAM Instance Profile (available when running inside EC2)
    config :access_key_id, :validate => :string

    # The AWS Secret Access Key
    config :secret_access_key, :validate => :password

    # The AWS Session token for temporary credential
    config :session_token, :validate => :password

    # URI to proxy server if required
    config :proxy_uri, :validate => :string

    # Custom endpoint to connect to s3
    config :endpoint, :validate => :string

    # The AWS IAM Role to assume, if any.
    # This is used to generate temporary credentials typically for cross-account access.
    # See https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html for more information.
    # When `role_arn` is set, AWS (`access_key_id`/`secret_access_key`) credentials still get used if they're configured.
    config :role_arn, :validate => :string

    # Session name to use when assuming an IAM role
    config :role_session_name, :validate => :string, :default => "logstash"

    # Path to YAML file containing a hash of AWS credentials.
    # This file will only be loaded if `access_key_id` and
    # `secret_access_key` aren't set. The contents of the
    # file should look like this:
    #
    # [source,ruby]
    # ----------------------------------
    #     :access_key_id: "12345"
    #     :secret_access_key: "54321"
    # ----------------------------------
    #
    config :aws_credentials_file, :validate => :string

    # By default, this plugin uses cert available to OpenSSL provided by OS
    # when verifying SSL peer certificates.
    # For cases where the default cert is unavailable, e.g. Windows,
    # you can use the bundled ca certificate provided by AWS SDK
    # by setting `use_aws_bundled_ca` to true
    config :use_aws_bundled_ca, :validate => :boolean, :default => false
  end
end
