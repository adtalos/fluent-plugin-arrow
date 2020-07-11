#
# Copyright 2018- joker1007
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'arrow'
require 'parquet'
require 'fluent/msgpack_factory'
require 'fluent/plugin/buffer/chunk'

module Fluent
  module Plugin
    class Buffer
      class ArrowMemoryChunk < Chunk
        def initialize(metadata, schema, chunk_size: 64*1024*1024, format: :arrow, codec: :text, compress: :text, log: nil)
          super(metadata, compress: compress)
          @schema = schema
          @chunk_size = chunk_size
          @format = format
          @codec = codec
          @log = log

          @record_batch_builder = nil
          @chunk = nil

          @chunk_bytes = 0
          @adding_bytes = 0
          @adding_size = 0
          @record_batch_builder_rows = 0

          reset
        end

        def concat(bulk, bulk_size)
          @record_batch_builder.append(Fluent::MessagePackFactory.engine_factory.unpacker.feed_each(bulk).filter { |value| value.is_a? Hash })
          @record_batch_builder_rows += bulk_size

          @adding_bytes += bulk.bytesize
          @adding_size += bulk_size
          true
        end

        def commit
          @size += @adding_size
          @chunk_bytes += @adding_bytes

          @adding_bytes = @adding_size = 0
          @modified_at = Fluent::Clock.real_now
          @modified_at_object = nil
          true
        end

        def rollback
          # unsupported
          false
        end

        def bytesize
          @chunk_bytes + @adding_bytes
        end

        def size
          @size + @adding_size
        end

        def empty?
          @record_batch_builder_rows == 0 && @chunk.nil?
        end

        def purge
          super
          @chunk_bytes = @size = @adding_bytes = @adding_size = @record_batch_builder_rows = 0

          @record_batch_builder = nil
          @chunk = nil
          true
        end

        def read(**kwargs)
          ensure_chunk
          @chunk.data
        end

        def open(**kwargs, &block)
          ensure_chunk
          StringIO.open(@chunk.data, &block)
        end

        def write_to(io, **kwargs)
          ensure_chunk
          io.write @chunk.data
        end

        private

        def reset
          @record_batch_builder = Arrow::RecordBatchBuilder.new(@schema)
          @record_batch_builder_rows = 0
        end

        def ensure_chunk
          if @chunk.nil?
            buffer = Arrow::ResizableBuffer.new(@chunk_size)
            output_stream = Arrow::BufferOutputStream.new(buffer)
            if @format == :parquet
              writer_properties = Parquet::WriterProperties.new
              if @codec != :text
                writer_properties.set_compression(@codec)
              end
              writer = Parquet::ArrowFileWriter.new(@schema, output_stream, writer_properties)
            else
              writer = Arrow::RecordBatchFileWriter.new(output_stream, @schema)
            end

            writer.write_table(@record_batch_builder.flush.to_table, @chunk_size)
            writer.close
            output_stream.close

            @chunk = buffer

            reset
          end
        end
      end
    end
  end
end
