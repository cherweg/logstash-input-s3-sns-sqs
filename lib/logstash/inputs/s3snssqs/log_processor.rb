# LogProcessor:
# reads and decodes locally available file with log lines
# and creates LogStash events from these
require 'logstash/inputs/mime/magic_gzip_validator'
require 'pathname'

module LogProcessor

  def self.included(base)
    base.extend(self)
  end

  def process(record, logstash_event_queue)
    file = record[:local_file]
    codec = @codec_factory.get_codec(record)
    folder = record[:folder]
    type = @type_by_folder.fetch(record[:bucket],{})[folder]
    metadata = {}
    line_count = 0
    event_count = 0
    #start_time = Time.now
    file_t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC) #PROFILING
    read_file(file) do |line|
      line_count += 1
      if stop?
        @logger.warn("[#{Thread.current[:name]}] Abort reading in the middle of the file, we will read it again when logstash is started")
        throw :skip_delete
      end
      begin
        codec.decode(line) do |event|
          event_count += 1
          decorate_event(event, metadata, type, record[:key], record[:bucket], record[:s3_data])
          #event_time = Time.now #PROFILING
          #event.set("[@metadata][progress][begin]", start_time)
          #event.set("[@metadata][progress][index_time]", event_time)
          #event.set("[@metadata][progress][line]", line_count)
          logstash_event_queue << event
        end
      rescue Exception => e
        @logger.error("[#{Thread.current[:name]}] Unable to decode line", :line => line, :error => e)
      end
    end
    file_t1 = Process.clock_gettime(Process::CLOCK_MONOTONIC) #PROFILING
    processing_time = (file_t1 - file_t0)
    #@logger.warn("[#{Thread.current[:name]}] Completed long running File ( took #{processing_time} ) s", file: record[:key], events: event_count, processing_time: processing_time  ) if processing_time > 600.0 #PROFILING
    # ensure any stateful codecs (such as multi-line ) are flushed to the queue
    codec.flush do |event|
      event_count += 1
      decorate_event(event, metadata, type, record[:key], record[:bucket], record[:s3_data])
      @logger.debug("[#{Thread.current[:name]}] Flushing an incomplete event", :event => event.to_s)
      logstash_event_queue << event
    end
    # signal completion:
    return true
  end

  private

  def decorate_event(event, metadata, type, key, bucket, s3_data)
    if event_is_metadata?(event)
      @logger.debug('Updating the current cloudfront metadata', :event => event)
      update_metadata(metadata, event)
    else
      # type by folder - set before "decorate()" enforces default
      event.set('type', type) if type and ! event.include?('type')
      decorate(event)

      event.set("cloudfront_version", metadata[:cloudfront_version]) unless metadata[:cloudfront_version].nil?
      event.set("cloudfront_fields", metadata[:cloudfront_fields]) unless metadata[:cloudfront_fields].nil?

      event.set("[@metadata][s3]", s3_data)
      event.set("[@metadata][s3][object_key]", key)
      event.set("[@metadata][s3][bucket_name]", bucket)
      event.set("[@metadata][s3][object_folder]", get_object_folder(key))

    end
  end

  def gzip?(filename)
    return true if filename.end_with?('.gz','.gzip')
    MagicGzipValidator.new(File.new(filename, 'rb')).valid?
  rescue Exception => e
    @logger.warn("Problem while gzip detection", :error => e)
  end

  def read_file(filename)
    zipped = gzip?(filename)
    completed = false
    file_stream = FileInputStream.new(filename)
    if zipped
      gzip_stream = GZIPInputStream.new(file_stream)
      decoder = InputStreamReader.new(gzip_stream, 'UTF-8')
    else
      decoder = InputStreamReader.new(file_stream, 'UTF-8')
    end
    buffered = BufferedReader.new(decoder)

    while (data = buffered.readLine())
      line = StringBuilder.new(data).append("\n")
      yield(line.toString())
    end
    completed = true
  rescue ZipException => e
    @logger.error("Gzip codec: We cannot uncompress the gzip file", :filename => filename, :error => e)
  ensure
    buffered.close unless buffered.nil?
    decoder.close unless decoder.nil?
    gzip_stream.close unless gzip_stream.nil?
    file_stream.close unless file_stream.nil?

    unless completed
      @logger.warn("[#{Thread.current[:name]}] Incomplete message in read_file. We´ll throw skip_delete.", :filename => filename)
      throw :skip_delete
    end

  end

  def event_is_metadata?(event)
    return false unless event.get("message").class == String
    line = event.get("message")
    version_metadata?(line) || fields_metadata?(line)
  end

  def version_metadata?(line)
    line.start_with?('#Version: ')
  end

  def fields_metadata?(line)
    line.start_with?('#Fields: ')
  end

  def update_metadata(metadata, event)
    line = event.get('message').strip

    if version_metadata?(line)
      metadata[:cloudfront_version] = line.split(/#Version: (.+)/).last
    end

    if fields_metadata?(line)
      metadata[:cloudfront_fields] = line.split(/#Fields: (.+)/).last
    end
  end
end
