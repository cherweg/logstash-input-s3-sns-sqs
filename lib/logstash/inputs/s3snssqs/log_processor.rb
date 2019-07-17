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
    type = @type_by_folder[folder] #if @type_by_folder.key?(folder)
    metadata = {}
    #@logger.info("processing file",:thread => Thread.current[:name], :local_file => record[:local_file])
    #@logger.info("[#{Thread.current[:name]}] start processing file (#{Time.now})", file: file, type: type, codec: codec) #PROFILING
    line_count = 0
    event_count = 0
    read_file(file) do |line|
      line_count += 1
      #@logger.info("[#{Thread.current[:name]}] process: received line #{line_count}") #PROFILING
      #@logger.info("got a yielded line", :line_count => line_count) if line_count < 10
      if stop?
        @logger.warn("[#{Thread.current[:name]}] Abort reading in the middle of the file, we will read it again when logstash is started")
        throw :skip_delete
      end
      #line_t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC) #PROFILING
      line = line.encode('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: "\u2370")
      #line_t1 = Process.clock_gettime(Process::CLOCK_MONOTONIC) #PROFILING
      #@logger.info("[#{Thread.current[:name]}] encoding line #{line_count} took #{(line_t1 - line_t0)} s") #PROFILING
      #@logger.info("ENcoded line", :line_count => line_count) if line_count < 10
      codec.decode(line) do |event|
        event_count += 1
        #event_t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC) #PROFILING
        decorate_event(event, metadata, type, record[:key], record[:bucket], folder)
        #event_t1 = Process.clock_gettime(Process::CLOCK_MONOTONIC) #PROFILING
        #@logger.info("[#{Thread.current[:name]}] decorating event #{event_count} took #{(event_t1 - event_t0)} s") #PROFILING
        logstash_event_queue << event
        #@logger.info("queued event ", :lines => line_count, :events => event_count, :thread => Thread.current[:name]) if event_count < 10
      end
      #@logger.info("DEcoded line", :line_count => line_count) if line_count < 10
    end
    #@logger.info("queued all events ", :lines => line_count, :events => event_count, :thread => Thread.current[:name])
    # ensure any stateful codecs (such as multi-line ) are flushed to the queue
    codec.flush do |event|
      event_count += 1
      #event_t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC) #PROFILING
      decorate_event(event, metadata, type, record[:key], record[:bucket], folder)
      #event_t1 = Process.clock_gettime(Process::CLOCK_MONOTONIC) #PROFILING
      #@logger.info("[#{Thread.current[:name]}] decorating flushed event #{event_count} took #{(event_t1 - event_t0)} s") #PROFILING
      @logger.debug("Flushing an incomplete event", :event => event.to_s)
      logstash_event_queue << event
    end
    # signal completion:
    return true
  end

  private

  def decorate_event(event, metadata, type, key, bucket, folder)
    if event_is_metadata?(event)
      @logger.debug('Updating the current cloudfront metadata', :event => event)
      update_metadata(metadata, event)
    else
      # type by folder - set before "decorate()" enforces default
      event.set('type', type) if type and ! event.include?('type')
      decorate(event)

      event.set("cloudfront_version", metadata[:cloudfront_version]) unless metadata[:cloudfront_version].nil?
      event.set("cloudfront_fields", metadata[:cloudfront_fields]) unless metadata[:cloudfront_fields].nil?

      event.set("[@metadata][s3][object_key]", key)
      event.set("[@metadata][s3][bucket_name]", bucket)
      event.set("[@metadata][s3][full_folder]", folder)
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
    completed = false
    zipped = gzip?(filename)
    file_stream = FileInputStream.new(filename)
    if zipped
      gzip_stream = GZIPInputStream.new(file_stream)
      decoder = InputStreamReader.new(gzip_stream, 'UTF-8')
    else
      decoder = InputStreamReader.new(file_stream, 'UTF-8')
    end
    buffered = BufferedReader.new(decoder)

    #@logger.info("[#{Thread.current[:name]}] read_file: start sending lines", file: filename, zipped: zipped) #PROFILING
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
    throw :skip_delete unless completed
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
