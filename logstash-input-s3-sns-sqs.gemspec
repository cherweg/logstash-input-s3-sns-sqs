Gem::Specification.new do |s|
  s.name            = 'logstash-input-s3-sns-sqs'
  s.version         = '2.1.0'
  s.licenses        = ['Apache-2.0']
  s.summary         = "Get logs from AWS s3 buckets as issued by an object-created event via sns -> sqs."
  s.description     = "This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.authors         = ["Christian Herweg"]
  s.email           = 'christian.herweg@gmail.com'
  s.homepage        = "https://github.com/cherweg/logstash-input-s3-sns-sqs"
  s.require_paths = ["lib"]

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "input" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 2.1.12", "<= 2.99"

  s.add_runtime_dependency 'logstash-codec-json', '~> 3.0'
  s.add_runtime_dependency 'logstash-mixin-aws', '~> 4.3'
  s.add_development_dependency 'logstash-codec-json_stream', '~> 1.0'
  s.add_development_dependency 'logstash-devutils', '~> 1.3'
end
