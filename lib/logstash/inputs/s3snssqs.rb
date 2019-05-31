# encoding: utf-8
require "logstash/inputs/threadable"
require "logstash/namespace"
require "logstash/timestamp"
require "logstash/plugin_mixins/aws_config"
require "logstash/errors"
require 'logstash/inputs/s3sqs/patch'
require "aws-sdk"
# "object-oriented interfaces on top of API clients"...
# => Overhead. FIXME: needed?
require "aws-sdk-resources"
require 'logstash/inputs/mime/MagicgzipValidator'
require "fileutils"
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
require_relative 'log_processor'

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

  config_name "s3snssqs"

  default :codec, "plain"

  # FIXME: needed per bucket in the future!
  # Future config might look somewhat like this:
  #
  # buckets {
  #   "bucket1_name": {
  #     "credentials": { "role": "aws:role:arn:for:bucket:access" },
  #     "folders": [
  #       {
  #         "prefix:": "key1",
  #         "codec": "some-codec"
  #       },
  #       {
  #         "prefix": "key2",
  #         "codec": "some-other-codec"
  #       }
  #     ]
  #   },
  #   "bucket2_name": {
  #     "credentials": {
  #        "access_key_id": "some-id",
  #        "secret_access_key": "some-secret-key"
  #     },
  #     "folders": [
  #       {
  #         "prefix": ""
  #       }
  #     ]
  #   }
  # }
  #
  ### s3 -> TODO: replace by options for multiple buckets
  config :s3_key_prefix, :validate => :string, :default => ''
  #Sometimes you need another key for s3. This is a first test...
  config :s3_access_key_id, :validate => :string
  config :s3_secret_access_key, :validate => :string
  config :set_role_by_bucket, :validate => :hash, :default => {}
  #If you have different file-types in you s3 bucket, you could define codec by folder
  #set_codec_by_folder => {"My-ELB-logs" => "plain"}
  config :set_codec_by_folder, :validate => :hash, :default => {}
  # The AWS IAM Role to assume, if any.
  # This is used to generate temporary credentials typically for cross-account access.
  # See https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html for more information.
  config :s3_role_arn, :validate => :string
  # Session name to use when assuming an IAM role
  config :s3_role_session_name, :validate => :string, :default => "logstash"

  ### sqs
  # Name of the SQS Queue to pull messages from. Note that this is just the name of the queue, not the URL or ARN.
  config :queue, :validate => :string, :required => true
  config :queue_owner_aws_account_id, :validate => :string, :required => false
  # Whether the event is processed though an SNS to SQS. (S3>SNS>SQS = true |S3>SQS=false)
  config :from_sns, :validate => :boolean, :default => true
  config :sqs_explicit_delete, :validate => :boolean, :default => false
  config :delete_on_success, :validate => :boolean, :default => false
  config :visibility_timeout, :validate => :number, :default => 600

  ### system
  # To run in multiple threads use this
  config :consumer_threads, :validate => :number, :default => 1
  config :temporary_directory, :validate => :string, :default => File.join(Dir.tmpdir, "logstash")

  # -> goes into LogProcessor:
  def set_codec (folder)
    begin
      @logger.debug("Automatically switching from #{@codec.class.config_name} to #{set_codec_by_folder[folder]} codec", :plugin => self.class.config_name)
      LogStash::Plugin.lookup("codec", "#{set_codec_by_folder[folder]}").new("charset" => @codec.charset)
    rescue Exception => e
      @logger.error("Failed to set_codec with error", :error => e)
    end
  end

  # this is the default; no need to explicitly define this:
  public

  # --- BEGIN plugin interface ----------------------------------------#

  # initialisation
  def register
    # prepare system
    FileUtils.mkdir_p(@temporary_directory) unless Dir.exist?(@temporary_directory)

    # make this hash do key lookups using regex matching
    hash_key_is_regex(@set_codec_by_folder)

    # instantiate helpers
    @sqs_poller = SqsPoller.new(@queue, {
      queue_owner_aws_account_id: @queue_owner_aws_account_id,
      from_sns: @from_sns,
      sqs_explicit_delete: @sqs_explicit_delete,
      visibility_timeout: @visibility_timeout,
      delete_on_success: @delete_on_success
    })
    @s3_client_factory = S3ClientFactory.new({
      aws_region: @region,
      s3_options_by_bucket: @s3_options_by_bucket,
      s3_role_session_name: @s3_role_session_name
    })
    @s3_downloader = S3Downloader.new(
      # FIXME: if we split the code this way, this needs the client factory!
      temporary_directory: @temporary_directory,
      s3_options_by_bucket: @s3_options_by_bucket
    )

    # administrative stuff
    @worker_threads = []
  end

  # startup
  def run(logstash_event_queue)
    # start them
    @worker_threads = @consumer_threads.times.map do |_|
      run_worker_thread(logstash_event_queue)
    end
    # and wait (possibly infinitely) for them to shut down
    @worker_threads.each { |t| t.join }
  end

  # shutdown
  def stop
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

  # --- END plugin interface ------------------------------------------#

  private

  def run_worker_thread(queue)
    Thread.new do
      @logger.info("Starting new worker thread")
      @sqs_poller.run do |record|
        logfile = s3_downloader.fetch(record)
        process_log(logfile) if file
      end
    end
  end

  private
  def process_log(bucket , key, folder, instance_codec, queue, message, size)
    s3bucket = @s3_resource.bucket(bucket)
    @logger.debug("Lets go reading file", :bucket => bucket, :key => key)
    object = s3bucket.object(key)
    filename = File.join(temporary_directory, File.basename(key))
    if download_remote_file(object, filename)
      if process_local_log( filename, key, folder, instance_codec, queue, bucket, message, size)
        begin
          FileUtils.remove_entry_secure(filename, true) if File.exists? filename
          delete_file_from_bucket(object)
        rescue Exception => e
          @logger.debug("Deleting file failed", :file => filename, :error => e)
        end
      end
    else
      begin
        FileUtils.remove_entry_secure(filename, true) if File.exists? filename
      rescue Exception => e
        @logger.debug("Cleaning up tmpdir failed", :file => filename, :error => e)
      end
    end
  end

  private
  # Stream the remove file to the local disk
  #
  # @param [S3Object] Reference to the remove S3 objec to download
  # @param [String] The Temporary filename to stream to.
  # @return [Boolean] True if the file was completely downloaded
  def download_remote_file(remote_object, local_filename)
    completed = false
    @logger.debug("S3 input: Download remote file", :remote_key => remote_object.key, :local_filename => local_filename)
    File.open(local_filename, 'wb') do |s3file|
      return completed if stop?
      begin
        remote_object.get(:response_target => s3file)
      rescue Aws::S3::Errors::ServiceError => e
        @logger.error("Unable to download file. We´ll requeue the message", :file => remote_object.inspect)
        throw :skip_delete
      end
    end
    completed = true

    return completed
  end

  private

  # Read the content of the local file
  #
  # @param [Queue] Where to push the event
  # @param [String] Which file to read from
  # @return [Boolean] True if the file was completely read, false otherwise.
  def process_local_log(filename, key, folder, instance_codec, queue, bucket, message, size)
    @logger.debug('Processing file', :filename => filename)
    metadata = {}
    start_time = Time.now
    # Currently codecs operates on bytes instead of stream.
    # So all IO stuff: decompression, reading need to be done in the actual
    # input and send as bytes to the codecs.
    read_file(filename) do |line|
      if (Time.now - start_time) >= (@visibility_timeout.to_f / 100.0 * 90.to_f)
        @logger.info("Increasing the visibility_timeout ... ", :timeout => @visibility_timeout, :filename => filename, :filesize => size, :start => start_time )
        poller.change_message_visibility_timeout(message, @visibility_timeout)
        start_time = Time.now
      end
      if stop?
        @logger.warn("Logstash S3 input, stop reading in the middle of the file, we will read it again when logstash is started")
        return false
      end
      line = line.encode('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: "\u2370")
      #@logger.debug("read line", :line => line)
      instance_codec.decode(line) do |event|
        @logger.debug("decorate event")
        # We are making an assumption concerning cloudfront
        # log format, the user will use the plain or the line codec
        # and the message key will represent the actual line content.
        # If the event is only metadata the event will be drop.
        # This was the behavior of the pre 1.5 plugin.
        #
        # The line need to go through the codecs to replace
        # unknown bytes in the log stream before doing a regexp match or
        # you will get a `Error: invalid byte sequence in UTF-8'
        local_decorate_and_queue(event, queue, key, folder, metadata, bucket)
      end
    end
    @logger.debug("end if file #{filename}")
    #@logger.info("event pre flush", :event => event)
    # #ensure any stateful codecs (such as multi-line ) are flushed to the queue
    instance_codec.flush do |event|
      local_decorate_and_queue(event, queue, key, folder, metadata, bucket)
      @logger.debug("We are about to flush an incomplete event...", :event => event)
    end

    return true
  end # def process_local_log

  private
  def local_decorate_and_queue(event, queue, key, folder, metadata, bucket)
    @logger.debug('decorating event', :event => event.to_s)
    if event_is_metadata?(event)
      @logger.debug('Event is metadata, updating the current cloudfront metadata', :event => event)
      update_metadata(metadata, event)
    else

      decorate(event)

      event.set("cloudfront_version", metadata[:cloudfront_version]) unless metadata[:cloudfront_version].nil?
      event.set("cloudfront_fields", metadata[:cloudfront_fields]) unless metadata[:cloudfront_fields].nil?

      event.set("[@metadata][s3][object_key]", key)
      event.set("[@metadata][s3][bucket_name]", bucket)
      event.set("[@metadata][s3][object_folder]", folder)
      @logger.debug('add metadata', :object_key => key, :bucket => bucket, :folder => folder)
      queue << event
    end
  end


  private
  def get_object_folder(key)
    if match=/#{s3_key_prefix}\/?(?<type_folder>.*?)\/.*/.match(key)
      return match['type_folder']
    else
      return ""
    end
  end

  private
  def read_file(filename, &block)
    if gzip?(filename)
      read_gzip_file(filename, block)
    else
      read_plain_file(filename, block)
    end
  end

  def read_plain_file(filename, block)
    File.open(filename, 'rb') do |file|
      file.each(&block)
    end
  end

  private
  def read_gzip_file(filename, block)
    file_stream = FileInputStream.new(filename)
    gzip_stream = GZIPInputStream.new(file_stream)
    decoder = InputStreamReader.new(gzip_stream, "UTF-8")
    buffered = BufferedReader.new(decoder)

    while (line = buffered.readLine())
      block.call(line)
    end
  rescue ZipException => e
    @logger.error("Gzip codec: We cannot uncompress the gzip file", :filename => filename, :error => e)
  ensure
    buffered.close unless buffered.nil?
    decoder.close unless decoder.nil?
    gzip_stream.close unless gzip_stream.nil?
    file_stream.close unless file_stream.nil?
  end

  private
  def gzip?(filename)
    return true if filename.end_with?('.gz','.gzip')
    MagicGzipValidator.new(File.new(filename, 'r')).valid?
  rescue Exception => e
    @logger.debug("Problem while gzip detection", :error => e)
  end

  private
  def delete_file_from_bucket(object)
    if @delete_on_success
      object.delete()
    end
  end


  private
  def get_s3client
    if s3_access_key_id and s3_secret_access_key
      @logger.debug("Using S3 Credentials from config", :ID => aws_options_hash.merge(:access_key_id => s3_access_key_id, :secret_access_key => s3_secret_access_key) )
      @s3_client = Aws::S3::Client.new(aws_options_hash.merge(:access_key_id => s3_access_key_id, :secret_access_key => s3_secret_access_key))
    elsif @s3_role_arn
      @s3_client = Aws::S3::Client.new(aws_options_hash.merge!({ :credentials => s3_assume_role }))
      @logger.debug("Using S3 Credentials from role", :s3client => @s3_client.inspect, :options => aws_options_hash.merge!({ :credentials => s3_assume_role }))
    else
      @s3_client = Aws::S3::Client.new(aws_options_hash)
    end
  end

  private
  def get_s3object
    s3 = Aws::S3::Resource.new(client: @s3_client)
  end

  private
  def s3_assume_role()
    Aws::AssumeRoleCredentials.new(
        client: Aws::STS::Client.new(region: @region),
        role_arn: @s3_role_arn,
        role_session_name: @s3_role_session_name
    )
  end

  private
  def event_is_metadata?(event)
    return false unless event.get("message").class == String
    line = event.get("message")
    version_metadata?(line) || fields_metadata?(line)
  end

  private
  def version_metadata?(line)
    line.start_with?('#Version: ')
  end

  private
  def fields_metadata?(line)
    line.start_with?('#Fields: ')
  end

  private
  def update_metadata(metadata, event)
    line = event.get('message').strip

    if version_metadata?(line)
      metadata[:cloudfront_version] = line.split(/#Version: (.+)/).last
    end

    if fields_metadata?(line)
      metadata[:cloudfront_fields] = line.split(/#Fields: (.+)/).last
    end
  end

  private

  def hash_key_is_regex(myhash)
    myhash.default_proc = lambda do |hash, lookup|
      result=nil
      hash.each_pair do |key, value|
        if %r[#{key}] =~ lookup
          result=value
          break
        end
      end
      result
    end
    # return imput hash (convenience)
    return myhash
  end

end # class
