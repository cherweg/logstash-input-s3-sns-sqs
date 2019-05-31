require "logstash/inputs/base"
require "logstash/inputs/s3/remote_file"

# MessagePoller polls messages from source queue
#

module LogStash module Inputs class S3SNSSQS < LogStash::Inputs::Base
  class Poller
    DEFAULT_OPTIONS = {
        # we will query 1 message at a time, so we can ensure correct error handling if we can't download a single file correctly
        # (we will throw :skip_delete if download size isn't correct to process the event again later
        # -> set a reasonable "Default Visibility Timeout" for your queue, so that there's enough time to process the log files)
        :max_number_of_messages => 1,
        # we will use the queue's setting, a good value is 10 seconds
        # (to ensure fast logstash shutdown on the one hand and few api calls on the other hand)
        :skip_delete => false,
        :visibility_timeout => @visibility_timeout,
        :wait_time_seconds => nil,
    }

    def initialize(queue, options = {})
      @runner_threads = []
      @queue = queue
      @stopped = false
      @options = DEFAULT_OPTIONS.merge(options)
      setup_queue
    end

    def setup_queue
      aws_sqs_client = Aws::SQS::Client.new(aws_options_hash)
      queue_url = aws_sqs_client.get_queue_url({ queue_name: @queue, queue_owner_aws_account_id: @queue_owner_aws_account_id})[:queue_url]
      @sqs_poller = Aws::SQS::QueuePoller.new(queue_url, :client => aws_sqs_client)
    rescue Aws::SQS::Errors::ServiceError => e
      @logger.error("Cannot establish connection to Amazon SQS", :error => e)
      raise LogStash::ConfigurationError, "Verify the SQS queue name and your credentials"
    end

    def run(&block)
        # ensure we can stop logstash correctly
        @runner_threads = consumer_threads.times.map { |consumer| thread_runner(queue) }
        @runner_threads.each { |t| t.join }
    end

    def stop
      @stopped = true
    end

    private
    attr_reader :options

    def thread_runner(queue)
      Thread.new do
        @logger.info("Starting new thread")
        begin
          poller.before_request do |stats|
            if stop? then
              @logger.warn("issuing :stop_polling on stop?", :queue => @queue)
              # this can take up to "Receive Message Wait Time" (of the sqs queue) seconds to be recognized
              throw :stop_polling
            end
          end
          # poll a message and process it
          run_with_backoff do
            @sqs_poller.poll(polling_options) do |message|
              begin
                handle_message(message, @codec.clone)
                poller.delete_message(message) if @sqs_explicit_delete
              rescue Exception => e
                @logger.info("Error in poller block ... ", :error => e)
              end
            end
          end
        end
      end
    end

    # Runs an AWS request inside a Ruby block with an exponential backoff in case
    # we experience a ServiceError.
    #
    # @param [Integer] max_time maximum amount of time to sleep before giving up.
    # @param [Integer] sleep_time the initial amount of time to sleep before retrying.
    # @param [Block] block Ruby code block to execute.
    def run_with_backoff(max_time = MAX_TIME_BEFORE_GIVING_UP, sleep_time = BACKOFF_SLEEP_TIME, &block)
      next_sleep = sleep_time
      begin
        block.call
        next_sleep = sleep_time
      rescue Aws::SQS::Errors::ServiceError => e
        @logger.warn("Aws::SQS::Errors::ServiceError ... retrying SQS request with exponential backoff", :queue => @queue, :sleep_time => sleep_time, :error => e)
        sleep(next_sleep)
        next_sleep =  next_sleep > max_time ? sleep_time : sleep_time * BACKOFF_FACTOR
        retry
      end
    end


    def stop?
      @stopped
    end
  end
end;end;end
