Gem::Specification.new do |s|
  s.name            = 'logstash-input-s3-sns-sqs'
  s.version         = '2.2.3'
  s.licenses        = ['Apache-2.0']
  s.summary         = "Get logs from AWS s3 buckets as issued by an object-created event via sns -> sqs."
  s.description     = "This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.authors         = ["Christian Herweg"]
  s.email           = 'christian.herweg@gmail.com'
  s.homepage        = "https://github.com/cherweg/logstash-input-s3-sns-sqs"
  s.require_paths = ["lib"]

  # JRuby 9.4+ tracks Ruby 3.1+. Logstash 8/9 ships JRuby 9.4 or 10.
  s.required_ruby_version = '>= 3.1'

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "input" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", "~> 2.0"
  # Note: cannot pin a higher floor because logstash-core itself constrains
  # concurrent-ruby (currently < 1.1.10). CVE-2026-54904/54905/54906 in
  # concurrent-ruby are not exploitable via this plugin: we only use
  # Concurrent::AtomicBoolean, not AtomicReference or ReadWriteLock.
  s.add_runtime_dependency "concurrent-ruby"
  s.add_runtime_dependency 'stud', '>= 0.0.22'
  s.add_runtime_dependency "logstash-codec-json"
  s.add_runtime_dependency "logstash-codec-plain"
  s.add_runtime_dependency "aws-sdk-core", "~> 3"
  s.add_runtime_dependency "aws-sdk-s3"
  s.add_runtime_dependency "aws-sdk-sqs"
  s.add_runtime_dependency "aws-sdk-sns"
  s.add_runtime_dependency "aws-sdk-resourcegroups"

  s.add_development_dependency "logstash-codec-json_lines"
  s.add_development_dependency "logstash-codec-multiline"
  s.add_development_dependency "logstash-codec-line"
  s.add_development_dependency 'logstash-devutils', '>= 0.0.16'
  s.add_development_dependency "logstash-input-generator"
  s.add_development_dependency "timecop"

end
