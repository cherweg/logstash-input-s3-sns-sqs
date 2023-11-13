# encoding: utf-8
require "logstash/config/mixin"

module LogStash::PluginMixins::AwsConfig
  require "logstash/plugin_mixins/aws_config/v2"

  US_EAST_1 = "us-east-1"
end
