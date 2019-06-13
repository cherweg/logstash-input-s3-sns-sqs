# MessagePoller:
# polling loop fetches messages from source queue and invokes
# the provided code block on them
require 'json'
require 'cgi'
require "logstash/inputs/threadable"

module LogStash module Inputs class S3SNSSQS < LogStash::Inputs::Threadable
  class SqsPoller

    # queue poller options we want to set explicitly
    DEFAULT_OPTIONS = {
        # we query one message at a time, so we can ensure correct error
        # handling if we can't download a single file correctly
        # (we will throw :skip_delete if download size isn't correct to allow
        # for processing the event again later, so make sure to set a reasonable
        # "DefaultVisibilityTimeout" for your queue so that there's enough time
        # to process the log files!)
        max_number_of_messages: 1,
        visibility_timeout: 600,
        # long polling; by default we use the queue's setting.
        # A good value is 10 seconds to to balance between a quick logstash
        # shutdown and fewer api calls.
        wait_time_seconds: nil,
        skip_delete: false,
    }

    # only needed in "run_with_backoff":
    BACKOFF_SLEEP_TIME = 1
    BACKOFF_FACTOR = 2
    MAX_TIME_BEFORE_GIVING_UP = 60
    # only needed in "preprocess":
    EVENT_SOURCE = 'aws:s3'
    EVENT_TYPE = 'ObjectCreated'

    # initialization and setup happens once, outside the threads:
    #
    def initialize(logger, sqs_queue, options = {}, aws_options_hash)
      @logger = logger
      @queue = sqs_queue
      # @stopped = false # FIXME: needed per thread?
      @from_sns = options[:from_sns]
      @options = DEFAULT_OPTIONS.merge(options.reject { |k| [:sqs_explicit_delete, :from_sns, :queue_owner_aws_account_id, :sqs_skip_delete].include? k })
      @options[:skip_delete] = options[:sqs_skip_delete]
      begin
        @logger.info("Registering SQS input", :queue => @queue)
        sqs_client = Aws::SQS::Client.new(aws_options_hash)
        queue_url = sqs_client.get_queue_url({
          queue_name: @queue,
          queue_owner_aws_account_id: @options[:queue_owner_aws_account_id]
        }).queue_url # is a method according to docs. Was [:queue_url].
        @poller = Aws::SQS::QueuePoller.new(queue_url,
          :client => sqs_client
        )
      rescue Aws::SQS::Errors::ServiceError => e
        @logger.error("Cannot establish connection to Amazon SQS", :error => e)
        raise LogStash::ConfigurationError, "Verify the SQS queue name and your credentials"
      end
    end

    #
    # this is called by every worker thread:
    #
    def run() # not (&block) - pass explicitly (use yield below)
      # per-thread timer to extend visibility if necessary
      extender = nil
      message_backoff = (@options[:visibility_timeout] * 90).to_f / 100.0
      new_visibility = 2 * @options[:visibility_timeout]

      # "shutdown handler":
      @poller.before_request do |_|
        if stop?
          # kill visibility extender thread if active?
          extender.kill if extender
          extender = nil
          @logger.warn('issuing :stop_polling on "stop?" signal', :queue => @queue)
          # this can take up to "Receive Message Wait Time" (of the sqs queue) seconds to be recognized
          throw :stop_polling
        end
      end

      run_with_backoff do
        @poller.poll(@options) do |message|
          @logger.info("Inside Poller: polled message", :message => message)
          # auto-double the timeout if processing takes too long:
          extender = Thread.new do
            sleep message_backoff
            @logger.info("Extending visibility for message", :message => message)
            @poller.change_message_visibility_timeout(message, new_visibility)
          end
          failed = false
          begin
            preprocess(message) do |record|
              @logger.info("we got a record", :record => record)
              yield(record) #unless record.nil? - unnecessary; implicit
            end
          rescue Exception => e
            @logger.warn("Error in poller loop", :error => e)
            failed = true
          end
          # at this time the extender has either fired or is obsolete
          extender.kill
          extender = nil
          throw :skip_delete if failed
        end
      end
    end

    # FIXME: this is not called at all, and not needed if "stop?" works as expected
    #
    def stop
      @stopped = true # FIXME: needed per thread?
    end

    def stop?
      @stopped
    end

    private

    def preprocess(message)
      @logger.debug("Inside Preprocess: Start", :message => message)
      payload = JSON.parse(message.body)
      payload = JSON.parse(payload['Message']) if @from_sns
      return nil unless payload['Records']
      payload['Records'].each do |record|
        @logger.debug("We found a record", :record => record)
        # in case there are any events with Records that aren't s3 object-created events and can't therefore be
        # processed by this plugin, we will skip them and remove them from queue
        if record['eventSource'] == EVENT_SOURCE and record['eventName'].start_with?(EVENT_TYPE) then
          @logger.debug("record is valid")
          yield({
            bucket: CGI.unescape(record['s3']['bucket']['name']),
            key: CGI.unescape(record['s3']['object']['key']),
            size: record['s3']['object']['size']
          })

          # -v- this stuff goes into s3 and processor handling: -v-

          # type_folder = get_object_folder(key)
          # Set input codec by :set_codec_by_folder
          # instance_codec = set_codec(type_folder) unless set_codec_by_folder["#{type_folder}"].nil?
          # try download and :skip_delete if it fails
          #if record['s3']['object']['size'] < 10000000 then
          # process_log(bucket, key, type_folder, instance_codec, queue, message, size)
          #else
          #  @logger.info("Your file is too big")
          #end
        end
      end
    end

    # Runs an AWS request inside a Ruby block with an exponential backoff in case
    # we experience a ServiceError.
    # @param [Integer] max_time maximum amount of time to sleep before giving up.
    # @param [Integer] sleep_time the initial amount of time to sleep before retrying.
    # instead of requiring
    # @param [Block] block Ruby code block to execute
    # and then doing a "block.call",
    # we yield to the passed block.
    def run_with_backoff(max_time = MAX_TIME_BEFORE_GIVING_UP, sleep_time = BACKOFF_SLEEP_TIME)
      next_sleep = sleep_time
      begin
        yield
        next_sleep = sleep_time
      rescue Aws::SQS::Errors::ServiceError => e
        @logger.warn("Aws::SQS::Errors::ServiceError ... retrying SQS request with exponential backoff", :queue => @queue, :sleep_time => sleep_time, :error => e)
        sleep(next_sleep)
        next_sleep = next_sleep > max_time ? sleep_time : sleep_time * BACKOFF_FACTOR
        retry
      end
    end

    # FIXME: really override? makes it necessary to call "stop()" above from outside
    # def stop?
    #   @stopped
    # end
  end
end;end;end
