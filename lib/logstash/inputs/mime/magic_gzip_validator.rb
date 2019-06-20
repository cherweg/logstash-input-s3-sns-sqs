class MagicGzipValidator
  attr_reader :file
  attr_reader :starting_signature

  VALID_STARTING_SIGNATURE = "1f8b"

  def initialize(file)
    raise "Expecting a file object as an argument" unless file.is_a?(File)

    # Ensure there are sufficient number of bytes to determine the
    # signature.
    if file.stat.size < minimum_bytes_for_determining_signature
      raise "File too small to calculate signature"
    end

    @file = file
    process_file!
  end

  def starting_signature_bytes
    2
  end

  def valid?
    @starting_signature == VALID_STARTING_SIGNATURE
  end
  private

  def minimum_bytes_for_determining_signature
    starting_signature_bytes
  end

  def process_file!
    read_starting_signature!

    # Ensure the file is closed after reading the starting signiture
    # bytes
    @file.close
  end

  def read_starting_signature!
    @file.rewind
    starting_bytes = @file.readpartial(starting_signature_bytes)
    @starting_signature = starting_bytes.unpack("H*").first
  end

end

#puts MagicGzipValidator.new(File.new('json.log', 'r')).valid?

#=> true

