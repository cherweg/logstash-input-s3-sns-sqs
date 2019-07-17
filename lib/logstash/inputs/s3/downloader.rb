# encoding: utf-8
require 'fileutils'
require 'thread'

class S3Downloader

  def initialize(logger, stop_semaphore, options)
    @logger = logger
    @stopped = stop_semaphore
    @factory = options[:s3_client_factory]
    @delete_on_success = options[:delete_on_success]
  end

  def copy_s3object_to_disk(record)
    # (from docs) WARNING:
    # yielding data to a block disables retries of networking errors!
    begin
      #download_t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC) #PROFILING
      #@logger.info("[#{Thread.current[:name]}] downloading file", file: record[:key]) #PROFILING
      @factory.get_s3_client(record[:bucket]) do |s3|
        #download_t1 = Process.clock_gettime(Process::CLOCK_MONOTONIC) #PROFILING
        #@logger.info("[#{Thread.current[:name]}] got s3 client after #{format('%.5f', download_t1 - download_t0)} s", file: record[:key]) #PROFILING
        response = s3.get_object(
          bucket: record[:bucket],
          key: record[:key],
          response_target: record[:local_file]
        )
        #download_t2 = Process.clock_gettime(Process::CLOCK_MONOTONIC) #PROFILING
        #@logger.info("[#{Thread.current[:name]}] download finished after #{format('%.5f', download_t2 - download_t1)} s", file: record[:key]) #PROFILING
      end
    rescue Aws::S3::Errors::ServiceError => e
      @logger.error("Unable to download file. Requeuing the message", :error => e, :record => record)
      # prevent sqs message deletion
      throw :skip_delete
    end
    throw :skip_delete if stop?
    return true
  end

  def cleanup_local_object(record)
    #@logger.info("Cleaning up file", :file => record[:local_file])
    FileUtils.remove_entry_secure(record[:local_file], true) if ::File.exists?(record[:local_file])
  rescue Exception => e
    @logger.warn("Could not delete file", :file => record[:local_file], :error => e)
  end

  def cleanup_s3object(record)
    return unless @delete_on_success
    begin
      @factory.get_s3_client(record[:bucket]) do |s3|
        s3.delete_object(bucket: record[:bucket], key: record[:key])
      end
    rescue Exception => e
      @logger.warn("Failed to delete s3 object", :record => record, :error => e)
    end
  end

  def stop?
    @stopped.value
  end

end # class
