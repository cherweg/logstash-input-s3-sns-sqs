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
    @logger.info("processing file",:thread => Thread.current[:name])
    line_count = 0
    event_count = 0
    read_file(file) do |line|
      line_count += 1
      if stop?
        @logger.warn("Abort reading in the middle of the file, we will read it again when logstash is started")
        throw :skip_delete
      end
      line = line.encode('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: "\u2370")
      codec.decode(line) do |event|
        decorate_event(event, metadata, type, record[:key], record[:bucket], folder)
        event_count += 1
        logstash_event_queue << event
      end
    end
    @logger.info("queued all events ", :lines => line_count, :events => event_count, :thread => Thread.current[:name])
    # ensure any stateful codecs (such as multi-line ) are flushed to the queue
    codec.flush do |event|
      decorate_event(event, metadata, type, record[:key], record[:bucket], folder)
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
      event.set('type', type) if type && !event.include?('type')
      decorate(event)

      event.set("cloudfront_version", metadata[:cloudfront_version]) unless metadata[:cloudfront_version].nil?
      event.set("cloudfront_fields", metadata[:cloudfront_fields]) unless metadata[:cloudfront_fields].nil?

      event.set("[@metadata][s3][object_key]", key)
      event.set("[@metadata][s3][bucket_name]", bucket)
      event.set("[@metadata][s3][full_folder]", folder)
      event.set("[@metadata][s3][object_folder]", get_object_folder(folder))
    end
  end

  def gzip?(filename)
    return true if filename.end_with?('.gz','.gzip')
    MagicGzipValidator.new(File.new(filename, 'rb')).valid?
  rescue Exception => e
    @logger.warn("Problem while gzip detection", :error => e)
  end

  def read_file(filename, &block)
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
    line = buffered.readLine()
    while (!line.nil?)
      block.call(line)
      line = buffered.readLine()
    end
    @logger.info("finished read_file",:thread => Thread.current[:name])
    completed = true

  rescue ZipException => e
    @logger.error("Gzip codec: We cannot uncompress the gzip file", :filename => filename, :error => e)
    return nil
  ensure
    buffered.close unless buffered.nil?
    decoder.close unless decoder.nil?
    gzip_stream.close unless gzip_stream.nil?
    file_stream.close unless file_stream.nil?
    throw :skip_delete unless completed
    return nil
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
