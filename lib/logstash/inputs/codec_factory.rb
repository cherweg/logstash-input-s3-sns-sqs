# CodecFactory:
# lazy-fetch codec plugins
# (FIXME: is this thread-safe?)
require "logstash/inputs/threadable"

#module LogStash module Inputs class S3SNSSQS < LogStash::Inputs::Threadable
class CodecFactory
  def initialize(logger, options)
    @logger = logger
    @default_codec = options[:default_codec]
    @codec_by_folder = options[:codec_by_folder]
    @codecs = {
      'default' => @default_codec
    }
  end

  def get_codec(record)
    codec = find_codec(record)
    if @codecs[codec].nil?
      @codecs[codec] = get_codec_plugin(codec)
    end
    @logger.debug("Switching to codec #{codec}") if codec != 'default'
    return @codecs[codec]
  end

  private

  def find_codec(record)
    bucket, key, folder = record[:bucket], record[:key], record[:folder]
    unless @codec_by_folder[bucket].nil?
      @logger.debug("trying to find codec for folder #{folder}", :codec =>  @codec_by_folder[bucket][folder])
      return @codec_by_folder[bucket][folder] unless @codec_by_folder[bucket][folder].nil?
    end
    return 'default'
  end

  def get_codec_plugin(name, options = {})
    LogStash::Plugin.lookup('codec', name).new(options)
  end
end
#end;end;end
