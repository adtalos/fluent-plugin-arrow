require "helper"
require "fluent/msgpack_factory"
require "fluent/plugin/buffer/arrow_memory_chunk"

class ArrowMemoryChunkTest < Test::Unit::TestCase
  setup do
    @fields = [
      ::Arrow::Field.new("key1", :uint64),
      ::Arrow::Field.new("key2", :double),
      ::Arrow::Field.new("key3", ::Arrow::TimestampDataType.new(:second)),
    ]
    @schema = Arrow::Schema.new(@fields)
    @c = Fluent::Plugin::Buffer::ArrowMemoryChunk.new(Object.new, @schema)
  end

  test "can #read" do
    d1 = {"key1" => 123, "key2" => 10.1234, "key3" => Fluent::EventTime.from_time(Time.now)}
    d2 = {"key1" => 124, "key2" => 11.1234, "key3" => Fluent::EventTime.from_time(Time.now)}
    data = [d1.to_msgpack, d2.to_msgpack]
    @c.append(data)
    ::Arrow::BufferInputStream.open(::Arrow::Buffer.new(@c.read)) do |input|
      reader = ::Arrow::RecordBatchFileReader.new(input)

      reader.each do |record_batch|
        assert { record_batch.n_rows == 2 }

        assert { record_batch.find_column(@fields[0].name).class == ::Arrow::UInt64Array }
        assert { record_batch.find_column(@fields[0].name).values == [123, 124] }
      end
    end
  end

  test "can #write_to" do
    time = Time.now
    d1 = {"key1" => 123, "key2" => 10.1234, "key3" => Fluent::EventTime.from_time(time)}
    d2 = {"key1" => 124, "key2" => 11.1234, "key3" => Fluent::EventTime.from_time(time)}
    data = [d1.to_msgpack, d2.to_msgpack]
    @c.append(data)
    Tempfile.create do |tf|
      @c.write_to(tf)
      tf.flush

      ::Arrow::MemoryMappedInputStream.open(tf.path) do |input|
        reader = ::Arrow::RecordBatchFileReader.new(input)
        reader.each_with_index do |record_batch, i|
          reader.each do |record_batch|
            assert { record_batch.n_rows == 2 }

            assert { record_batch.find_column(@fields[0].name).class == ::Arrow::UInt64Array }
            assert { record_batch.find_column(@fields[0].name).values == [123, 124] }
            assert { record_batch.find_column(@fields[1].name).values == [10.1234, 11.1234] }
            assert { record_batch.find_column(@fields[2].name)[0].to_i == time.to_i }
          end
        end
      end
    end
  end
end
