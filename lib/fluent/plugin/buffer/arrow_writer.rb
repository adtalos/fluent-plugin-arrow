module Fluent
  module Plugin
    class Buffer
      module ArrowWriter

        def concat(bulk, bulk_size)
          writed_bytes = write_arrow bulk
          @adding_bytes += writed_bytes
          @adding_size += bulk_size
          true
        end

        private

        def ensure_close
          @writer&.close
          @output_stream&.close
        end

        def init_arrow(output_stream)
          if @store_as == :gzip
            codec = Arrow::Codec.new(:gzip)
            output_stream = Arrow::CompressedOutputStream.new(codec, output_stream)
          end
          if @format == :parquet
            @writer = Parquet::ArrowFileWriter.new(@schema, output_stream)
          else
            @writer = Arrow::RecordBatchFileWriter.new(output_stream, @schema)
          end
          output_stream
        end

        def write_arrow(bulk)
          before = @output_stream.tell

          record_batch = ::Arrow::RecordBatch.new(@schema, Fluent::MessagePackFactory.engine_factory.unpacker.feed_each(bulk))
          @writer.write_table(record_batch.to_table, @chunk_size)

          return @output_stream.tell - before
        end
      end
    end
  end
end
