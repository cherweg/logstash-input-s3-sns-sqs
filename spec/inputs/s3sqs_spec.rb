# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/plugin"
require "logstash/inputs/s3snssqs"
require "fileutils"
require "logstash/errors"
require "logstash/event"
require "logstash/json"
require "logstash/codecs/base"
require "logstash/codecs/json_stream"
require 'rspec'
require 'rspec/expectations'



describe LogStash::Inputs::S3SNSSQS do
  class LogStash::Inputs::S3SNSSQS
    public :process # use method without error logging for better visibility of errors
  end
  let(:codec_options) { {} }

  let(:input) { LogStash::Inputs::S3SNSSQS.new(config) }

  let(:codec_factory) { CodecFactory.new(@logger, { default_codec: @codec, codec_by_folder: @codec_by_folder }) }
  subject { input }

  context "default parser choice" do
    it "should return true" do
      expect(true).to be true
    end
  end

  let(:record) {{"local_file" => File.join(File.dirname(__FILE__), '..', '..', 'fixtures', 'log-stream.real-formatted') }}
  let(:key) { "arn:aws:iam::123456789012:role/AuthorizedRole" }
  let(:folder) { "arn:aws:iam::123456789012:role/AuthorizedRole" }
  let(:instance_codec) { "json" }
  let(:logstash_event_queue) { "arn:aws:iam::123456789012:role/AuthorizedRole" }
  let(:bucket) { "arn:aws:iam::123456789012:role/AuthorizedRole" }
  let(:message) { "arn:aws:iam::123456789012:role/AuthorizedRole" }
  let(:size) { "123344" }
  let(:temporary_directory) { Stud::Temporary.pathname }
  let(:config) { {"queue" => queue, "codec" => "json", "temporary_directory" => temporary_directory } }
  context 'compressed_log_file' do

    subject do
      LogStash::Inputs::S3SNSSQS.new(config)
    end
 #   end
    let(:queue) { [] }
    before do
      @codec = LogStash::Codecs::JSONStream.new
      @codec.charset = "UTF-8"
      @codec_factory = CodecFactory.new(@logger, {
          default_codec: @codec,
          codec_by_folder: @codec_by_folder
      })
      expect( subject.process(record, logstash_event_queue) ).to be true
      $stderr.puts "method #{queue.to_s}"
    end

    #it '.process_local_log => process compressed log file and verfied logstash event queue with the correct number of events' do
    #  expect( queue.size ).to eq(38)
    #  expect( queue.clear).to be_empty
    #end
  end
end