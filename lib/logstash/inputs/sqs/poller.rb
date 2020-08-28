# MessagePoller:
# polling loop fetches messages from source queue and invokes
# the provided code block on them
require 'json'
require 'cgi'

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
      visibility_timeout: 10,
      # long polling; by default we use the queue's setting.
      # A good value is 10 seconds to to balance between a quick logstash
      # shutdown and fewer api calls.
      wait_time_seconds: nil,
      #attribute_names: ["All"], # Receive all available built-in message attributes.
      #message_attribute_names: ["All"], # Receive any custom message attributes.
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
  def initialize(logger, stop_semaphore, poller_options = {}, client_options = {}, aws_options_hash)
    @logger = logger
    @stopped = stop_semaphore
    @queue = client_options[:sqs_queue]
    @from_sns = client_options[:from_sns]
    @max_processing_time = client_options[:max_processing_time]
    @sqs_delete_on_failure = client_options[:sqs_delete_on_failure]
    @options = DEFAULT_OPTIONS.merge(poller_options)
    begin
      @logger.info("Registering SQS input", :queue => @queue)
      sqs_client = Aws::SQS::Client.new(aws_options_hash)
      if uri?(@queue)
        queue_url = @queue
      else
        queue_url = sqs_client.get_queue_url({
          queue_name: @queue,
          queue_owner_aws_account_id: client_options[:queue_owner_aws_account_id]
        }).queue_url
      end
      @poller = Aws::SQS::QueuePoller.new(queue_url,
        :client => sqs_client
      )
    rescue Aws::SQS::Errors::ServiceError => e
      @logger.error("Cannot establish connection to Amazon SQS", :error => e)
      raise LogStash::ConfigurationError, "Verify the SQS queue name and your credentials"
    end
  end

  # this is called by every worker thread:
  def run() # not (&block) - pass explicitly (use yield below)
    # per-thread timer to extend visibility if necessary
    extender = nil
    message_backoff = (@options[:visibility_timeout] * 95).to_f / 100.0
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
      message_count = 0 #PROFILING
      @poller.poll(@options) do |message|
        message_count += 1 #PROFILING
        message_t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC) #PROFILING
        # auto-increase the timeout if processing takes too long:
        poller_thread = Thread.current
        extender = Thread.new do
          while new_visibility < @max_processing_time do

            sleep message_backoff
            begin
              @poller.change_message_visibility_timeout(message, new_visibility)
              @logger.warn("[#{Thread.current[:name]}] Extended visibility for a long running message", :visibility => new_visibility) if new_visibility > 600.0
              new_visibility += message_backoff
            rescue Aws::SQS::Errors::InvalidParameterValue => e
              @logger.debug("Extending visibility failed for message", :error => e)
            else
              @logger.debug("[#{Thread.current[:name]}] Extended visibility for message", :visibility => new_visibility) #PROFILING
            end
          end
          @logger.error("[#{Thread.current[:name]}] Maximum visibility reached! We will delete this message from queue!")
          @poller.delete_message(message) if @sqs_delete_on_failure
          poller_thread.kill
        end
        extender[:name] = "#{Thread.current[:name]}/extender" #PROFILING
        failed = false
        record_count = 0
        begin
          message_completed = catch(:skip_delete) do
            preprocess(message) do |record|
              record_count += 1
              extender[:name] = "#{Thread.current[:name]}/extender/#{record[:key]}" #PROFILING
              yield(record)
            end
          end
        rescue Exception => e
          @logger.warn("Error in poller loop", :error => e)
          @logger.warn("Backtrace:\n\t#{e.backtrace.join("\n\t")}")
          failed = true
        end
        message_t1 = Process.clock_gettime(Process::CLOCK_MONOTONIC) #PROFILING
        unless message_completed
          @logger.debug("[#{Thread.current[:name]}] uncompleted message at the end of poller loop. We´ll throw skip_delete.", :message_count => message_count)
          extender.run if extender
        end
        # at this time the extender has either fired or is obsolete
        extender.kill if extender
        extender = nil
        throw :skip_delete if failed or ! message_completed
        #@logger.info("[#{Thread.current[:name]}] completed message.", :message => message_count)
      end
    end
  end

  private

  def stop?
    @stopped.value
  end

  def preprocess(message)
    @logger.debug("Inside Preprocess: Start", :event => message)
    payload = JSON.parse(message.body)
    payload = JSON.parse(payload['Message']) if @from_sns
    @logger.debug("Payload in Preprocess: ", :payload => payload)
    return nil unless payload['Records']
    payload['Records'].each do |record|
      @logger.debug("We found a record", :record => record)
      # in case there are any events with Records that aren't s3 object-created events and can't therefore be
      # processed by this plugin, we will skip them and remove them from queue
      if record['eventSource'] == EVENT_SOURCE and record['eventName'].start_with?(EVENT_TYPE) then
        @logger.debug("record is valid")
        bucket  = CGI.unescape(record['s3']['bucket']['name'])
        key     = CGI.unescape(record['s3']['object']['key'])
        size    = record['s3']['object']['size']
        yield({
          bucket: bucket,
          key: key,
          size: size,
          folder: get_object_path(key)
        })
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

  def uri?(string)
    uri = URI.parse(string)
    %w( http https ).include?(uri.scheme)
  rescue URI::BadURIError
    false
  rescue URI::InvalidURIError
    false
  end


  def get_object_path(key)
    folder = ::File.dirname(key)
    return '' if folder == '.'
    return folder
  end

end
