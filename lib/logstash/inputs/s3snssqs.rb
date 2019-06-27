# encoding: utf-8
require "logstash/inputs/threadable"
require "logstash/namespace"
require "logstash/timestamp"
require "logstash/plugin_mixins/aws_config"
require "logstash/shutdown_watcher"
require "logstash/errors"
require 'logstash/inputs/s3sqs/patch'
require "aws-sdk"

# "object-oriented interfaces on top of API clients"...
# => Overhead. FIXME: needed?
#require "aws-sdk-resources"
require "fileutils"
require "concurrent"
require 'tmpdir'
# unused in code:
#require "stud/interval"
#require "digest/md5"

require 'java'
java_import java.io.InputStream
java_import java.io.InputStreamReader
java_import java.io.FileInputStream
java_import java.io.BufferedReader
java_import java.util.zip.GZIPInputStream
java_import java.util.zip.ZipException

# our helper classes
# these may go into this file for brevity...
require_relative 'sqs/poller'
require_relative 's3/client_factory'
require_relative 's3/downloader'
require_relative 'codec_factory'
require_relative 's3snssqs/log_processor'

Aws.eager_autoload!

# Get logs from AWS s3 buckets as issued by an object-created event via sqs.
#
# This plugin is based on the logstash-input-sqs plugin but doesn't log the sqs event itself.
# Instead it assumes, that the event is an s3 object-created event and will then download
# and process the given file.
#
# Some issues of logstash-input-sqs, like logstash not shutting down properly, have been
# fixed for this plugin.
#
# In contrast to logstash-input-sqs this plugin uses the "Receive Message Wait Time"
# configured for the sqs queue in question, a good value will be something like 10 seconds
# to ensure a reasonable shutdown time of logstash.
# Also use a "Default Visibility Timeout" that is high enough for log files to be downloaded
# and processed (I think a good value should be 5-10 minutes for most use cases), the plugin will
# avoid removing the event from the queue if the associated log file couldn't be correctly
# passed to the processing level of logstash (e.g. downloaded content size doesn't match sqs event).
#
# This plugin is meant for high availability setups, in contrast to logstash-input-s3 you can safely
# use multiple logstash nodes, since the usage of sqs will ensure that each logfile is processed
# only once and no file will get lost on node failure or downscaling for auto-scaling groups.
# (You should use a "Message Retention Period" >= 4 days for your sqs to ensure you can survive
# a weekend of faulty log file processing)
# The plugin will not delete objects from s3 buckets, so make sure to have a reasonable "Lifecycle"
# configured for your buckets, which should keep the files at least "Message Retention Period" days.
#
# A typical setup will contain some s3 buckets containing elb, cloudtrail or other log files.
# These will be configured to send object-created events to a sqs queue, which will be configured
# as the source queue for this plugin.
# (The plugin supports gzipped content if it is marked with "contend-encoding: gzip" as it is the
# case for cloudtrail logs)
#
# The logstash node therefore must have sqs permissions + the permissions to download objects
# from the s3 buckets that send events to the queue.
# (If logstash nodes are running on EC2 you should use a ServerRole to provide permissions)
# [source,json]
#   {
#       "Version": "2012-10-17",
#       "Statement": [
#           {
#               "Effect": "Allow",
#               "Action": [
#                   "sqs:Get*",
#                   "sqs:List*",
#                   "sqs:ReceiveMessage",
#                   "sqs:ChangeMessageVisibility*",
#                   "sqs:DeleteMessage*"
#               ],
#               "Resource": [
#                   "arn:aws:sqs:us-east-1:123456789012:my-elb-log-queue"
#               ]
#           },
#           {
#               "Effect": "Allow",
#               "Action": [
#                   "s3:Get*",
#                   "s3:List*",
#                   "s3:DeleteObject"
#               ],
#               "Resource": [
#                   "arn:aws:s3:::my-elb-logs",
#                   "arn:aws:s3:::my-elb-logs/*"
#               ]
#           }
#       ]
#   }
#
class LogStash::Inputs::S3SNSSQS < LogStash::Inputs::Threadable
  include LogStash::PluginMixins::AwsConfig::V2
  include LogProcessor

  config_name "s3snssqs"

  default :codec, "plain"



  # Future config might look somewhat like this:
  #
  # s3_options_by_bucket = [
  #   {
  #     "bucket_name": "my-beautiful-bucket",
  #     "credentials": { "role": "aws:role:arn:for:bucket:access" },
  #     "folders": [
  #       {
  #         "key": "my_folder",
  #         "codec": "json"
  #         "type": "my_lovely_index"
  #       },
  #       {
  #         "key": "my_other_folder",
  #         "codec": "json_stream"
  #         "type": ""
  #       }
  #     ]
  #   },
  #   {
  #     "bucket_name": "my-other-bucket"
  #     "credentials": {
  #        "access_key_id": "some-id",
  #        "secret_access_key": "some-secret-key"
  #     },
  #     "folders": [
  #       {
  #         "key": ""
  #       }
  #     ]
  #   }
  # }

  config :s3_key_prefix, :validate => :string, :default => '', :deprecated => true #, :obsolete => " Will be moved to s3_options_by_bucket/types"

  config :s3_access_key_id, :validate => :string, :deprecated => true #, :obsolete => "Please migrate to :s3_options_by_bucket. We will remove this option in the next Version"
  config :s3_secret_access_key, :validate => :string, :deprecated => true #, :obsolete => "Please migrate to :s3_options_by_bucket. We will remove this option in the next Version"
  config :s3_role_arn, :validate => :string, :deprecated => true #, :obsolete => "Please migrate to :s3_options_by_bucket. We will remove this option in the next Version"

  config :set_codec_by_folder, :validate => :hash, :default => {}, :deprecated => true #, :obsolete => "Please migrate to :s3_options_by_bucket. We will remove this option in the next Version"

  # Default Options for the S3 clients
  config :s3_default_options, :validate => :hash, :required => false, :default => {}
  # We need a list of buckets, together with role arns and possible folder/codecs:
  config :s3_options_by_bucket, :validate => :array, :required => false # TODO: true
  # Session name to use when assuming an IAM role
  config :s3_role_session_name, :validate => :string, :default => "logstash"

  ### sqs
  # Name of the SQS Queue to pull messages from. Note that this is just the name of the queue, not the URL or ARN.
  config :queue, :validate => :string, :required => true
  config :queue_owner_aws_account_id, :validate => :string, :required => false
  # Whether the event is processed though an SNS to SQS. (S3>SNS>SQS = true |S3>SQS=false)
  config :from_sns, :validate => :boolean, :default => true
  config :sqs_skip_delete, :validate => :boolean, :default => false
  config :delete_on_success, :validate => :boolean, :default => false
  config :visibility_timeout, :validate => :number, :default => 600

  ### system
  config :temporary_directory, :validate => :string, :default => File.join(Dir.tmpdir, "logstash")
  # To run in multiple threads use this
  config :consumer_threads, :validate => :number, :default => 1


  public

  # --- BEGIN plugin interface ----------------------------------------#

  # initialisation
  def register
    # prepare system
    FileUtils.mkdir_p(@temporary_directory) unless Dir.exist?(@temporary_directory)

    @credentials_by_bucket = hash_key_is_regex({})
    # create the bucket=>folder=>codec lookup from config options
    @codec_by_folder = hash_key_is_regex({})
    @type_by_folder = hash_key_is_regex({})

    # use deprecated settings only if new config is missing:
    if @s3_options_by_bucket.nil?
      # We don't know any bucket name, so we must rely on a "catch-all" regex
      s3_options = {
        'bucket_name' => '.*',
        'folders' => @set_codec_by_folder.map { |key, codec|
          { 'key' => key, 'codec' => codec }
        }
      }
      if @s3_role_arn.nil?
        # access key/secret key pair needed
        unless @s3_access_key_id.nil? or @s3_secret_access_key.nil?
          s3_options['credentials'] = {
            'access_key_id' => @s3_access_key_id,
            'secret_access_key' => @s3_secret_access_key
          }
        end
      else
        s3_options['credentials'] = {
          'role' => @s3_role_arn
        }
      end
      @s3_options_by_bucket = [s3_options]
    end

    @s3_options_by_bucket.each do |options|
      bucket = options['bucket_name']
      if options.key?('credentials')
        @credentials_by_bucket[bucket] = options['credentials']
      end
      if options.key?('folders')
        # make these hashes do key lookups using regex matching
        folders = hash_key_is_regex({})
        types = hash_key_is_regex({})
        options['folders'].each do |entry|
          @logger.debug("options for folder ", :folder => entry)
          folders[entry['key']] = entry['codec'] if entry.key?('codec')
          types[entry['key']] = entry['type'] if entry.key?('type')
        end
        @codec_by_folder[bucket] = folders unless folders.empty?
        @type_by_folder[bucket] = types unless types.empty?
      end
    end

    @received_stop = Concurrent::AtomicBoolean.new(false)

    # instantiate helpers
    @sqs_poller = SqsPoller.new(@logger, @received_stop, @queue, {
      queue_owner_aws_account_id: @queue_owner_aws_account_id,
      from_sns: @from_sns,
      sqs_explicit_delete: @sqs_explicit_delete,
      visibility_timeout: @visibility_timeout
    }, aws_options_hash)
    @s3_client_factory = S3ClientFactory.new(@logger, {
      aws_region: @region,
      s3_default_options: @s3_default_options,
      s3_credentials_by_bucket: @credentials_by_bucket,
      s3_role_session_name: @s3_role_session_name
    }, aws_options_hash)
    @s3_downloader = S3Downloader.new(@logger, @received_stop, {
      s3_client_factory: @s3_client_factory,
      delete_on_success: @delete_on_success
    })
    @codec_factory = CodecFactory.new(@logger, {
      default_codec: @codec,
      codec_by_folder: @codec_by_folder
    })
    #@log_processor = LogProcessor.new(self)

    # administrative stuff
    @worker_threads = []
  end

  # startup
  def run(logstash_event_queue)
    #LogStash::ShutdownWatcher.abort_threshold(30)
    # start them
    @queue_mutex = Mutex.new
    #@consumer_threads= 1
    @worker_threads = @consumer_threads.times.map do |thread_id|
      run_worker_thread(logstash_event_queue, thread_id)
    end
    # and wait (possibly infinitely) for them to shut down
    @worker_threads.each { |t| t.join }
  end

  # shutdown
  def stop
    @received_stop.make_true
    @worker_threads.each do |worker|
      begin
        @logger.info("Stopping thread ... ", :thread => worker.inspect)
        worker.wakeup
      rescue
        @logger.error("Cannot stop thread ... try to kill him", :thread => worker.inspect)
        worker.kill
      end
    end
  end

  def stop?
    @received_stop.value
  end

  # --- END plugin interface ------------------------------------------#

  private

  def run_worker_thread(queue, thread_id)
    Thread.new do
      @logger.info("Starting new worker thread")
      temporary_directory = Dir.mktmpdir("#{@temporary_directory}/")
      @sqs_poller.run do |record|
        throw :skip_delete if stop?
        @logger.debug("Outside Poller: got a record", :record => record)
        # record is a valid object with the keys ":bucket", ":key", ":size"
        record[:local_file] = File.join(temporary_directory, File.basename(record[:key]))
        LogStash::Util.set_thread_name("[Processor #{thread_id} -  Working on: #{record[:key]}")
        if @s3_downloader.copy_s3object_to_disk(record)
          completed = catch(:skip_delete) do
            process(record, queue)
          end
          @s3_downloader.cleanup_local_object(record)
          # re-throw if necessary:
          throw :skip_delete unless completed
          @s3_downloader.cleanup_s3object(record)
        end
      end
    end
  end

  # Will be remove in further releases...
  def get_object_folder(key)
    if match = /#{s3_key_prefix}\/?(?<type_folder>.*?)\/.*/.match(key)
      return match['type_folder']
    else
      return ""
    end
  end

  def hash_key_is_regex(myhash)
    myhash.default_proc = lambda do |hash, lookup|
      result=nil
      hash.each_pair do |key, value|
        if %r[#{key}] =~ lookup
          result = value
          break
        end
      end
      result
    end
    # return input hash (convenience)
    return myhash
  end
end # class
