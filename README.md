# Logstash Plugin

This is a plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0.

## Documentation

 Get logs from AWS s3 buckets as issued by an object-created event via sqs.

 This plugin is based on the logstash-input-sqs plugin but doesn't log the sqs event itself.
 Instead it assumes, that the event is an s3 object-created event and will then download
 and process the given file.

 Some issues of logstash-input-sqs, like logstash not shutting down properly, have been
 fixed for this plugin.

 In contrast to logstash-input-sqs this plugin uses the "Receive Message Wait Time"
 configured for the sqs queue in question, a good value will be something like 10 seconds
 to ensure a reasonable shutdown time of logstash.
 Also use a "Default Visibility Timeout" that is high enough for log files to be downloaded
 and processed (I think a good value should be 5-10 minutes for most use cases), the plugin will
 avoid removing the event from the queue if the associated log file couldn't be correctly
 passed to the processing level of logstash (e.g. downloaded content size doesn't match sqs event).

 This plugin is meant for high availability setups, in contrast to logstash-input-s3 you can safely
 use multiple logstash nodes, since the usage of sqs will ensure that each logfile is processed
 only once and no file will get lost on node failure or downscaling for auto-scaling groups.
 (You should use a "Message Retention Period" >= 4 days for your sqs to ensure you can survive
 a weekend of faulty log file processing)
 The plugin will not delete objects from s3 buckets, so make sure to have a reasonable "Lifecycle"
 configured for your buckets, which should keep the files at least "Message Retention Period" days.

 A typical setup will contain some s3 buckets containing elb, cloudtrail or other log files.
 These will be configured to send object-created events to a sqs queue, which will be configured
 as the source queue for this plugin.
 (The plugin supports gzipped content if it is marked with "contend-encoding: gzip" as it is the
 case for cloudtrail logs)

 The logstash node therefore must have sqs permissions + the permissions to download objects
 from the s3 buckets that send events to the queue.
 (If logstash nodes are running on EC2 you should use a ServerRole to provide permissions)
 [source,json]
   {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Effect": "Allow",
               "Action": [
                   "sqs:Get*",
                   "sqs:List*",
                   "sqs:ReceiveMessage",
                   "sqs:ChangeMessageVisibility*",
                   "sqs:DeleteMessage*"
               ],
               "Resource": [
                   "arn:aws:sqs:us-east-1:123456789012:my-elb-log-queue"
               ]
           },
           {
               "Effect": "Allow",
               "Action": [
                   "s3:Get*",
                   "s3:List*",
                   "s3:DeleteObject"
               ],
               "Resource": [
                   "arn:aws:s3:::my-elb-logs",
                   "arn:aws:s3:::my-elb-logs/*"
               ]
           }
       ]
   }

## Need Help?

Need help? Try #logstash on freenode IRC or the https://discuss.elastic.co/c/logstash discussion forum.

## Developing

### 1. Plugin Developement and Testing

#### Code
- To get started, you'll need JRuby with the Bundler gem installed.

- Create a new plugin or clone and existing from the GitHub [logstash-plugins](https://github.com/logstash-plugins) organization. We also provide [example plugins](https://github.com/logstash-plugins?query=example).

- Install dependencies
```sh
bundle install
```

#### Test

- Update your dependencies

```sh
bundle install
```

- Run tests

```sh
bundle exec rspec
```

### 2. Running your unpublished Plugin in Logstash

#### 2.1 Run in a local Logstash clone

- Edit Logstash `Gemfile` and add the local plugin path, for example:
```ruby
gem "logstash-filter-awesome", :path => "/your/local/logstash-filter-awesome"
```
- Install plugin
```sh
bin/plugin install --no-verify
```
- Run Logstash with your plugin
```sh
bin/logstash -e 'filter {awesome {}}'
```
At this point any modifications to the plugin code will be applied to this local Logstash setup. After modifying the plugin, simply rerun Logstash.

#### 2.2 Run in an installed Logstash

You can use the same **2.1** method to run your plugin in an installed Logstash by editing its `Gemfile` and pointing the `:path` to your local plugin development directory or you can build the gem and install it using:

- Build your plugin gem
```sh
gem build logstash-filter-awesome.gemspec
```
- Install the plugin from the Logstash home
```sh
bin/plugin install /your/local/plugin/logstash-filter-awesome.gem
```
- Start Logstash and proceed to test the plugin

## Contributing

All contributions are welcome: ideas, patches, documentation, bug reports, complaints, and even something you drew up on a napkin.

Programming is not a required skill. Whatever you've seen about open source and maintainers or community members  saying "send patches or die" - you will not see that here.

It is more important to the community that you are able to contribute.

For more information about contributing, see the [CONTRIBUTING](https://github.com/elastic/logstash/blob/master/CONTRIBUTING.md) file.
