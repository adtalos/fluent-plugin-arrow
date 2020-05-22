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
require 'fluent/plugin/buffer/arrow_writer'

module Fluent
  module Plugin
    class Buffer
      class ArrowMemoryChunk < Chunk
        include ArrowWriter

        def initialize(metadata, schema, chunk_size: 8192, format: :arrow, store_as: :text, log: nil)
          super(metadata, compress: :text)
          @schema = schema
          @chunk_size = chunk_size
          @format = format
          @store_as = store_as
          @log = log

          @chunk = nil
          @output_stream = nil
          @writer = nil
          @staging_chunk = nil

          reset
        end

        def commit
          @size += @adding_size
          @writer_bytes += @adding_bytes

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
          @writer_bytes + @adding_bytes
        end

        def size
          @size + @adding_size
        end

        def empty?
          @output_stream.tell == 0
        end

        def purge
          super
          ensure_close
          true
        end

        def read(**kwargs)
          ensure_stage
          @staging_chunk.data
        end

        def staged!
          @staging_chunk = nil
          super
        end

        def open(**kwargs, &block)
          ensure_stage
          StringIO.open(@staging_chunk.data, &block)
        end

        def close
          super
          ensure_close
        end

        private

        def reset
          @chunk = Arrow::ResizableBuffer.new(@chunk_size)
          @output_stream = Arrow::BufferOutputStream.new(@chunk)

          @output_stream = init_arrow @output_stream

          @writer_bytes = 0
          @adding_bytes = 0
          @adding_size = 0
        end

        def ensure_stage
          if @staging_chunk.nil?
            ensure_close
            @staging_chunk = @chunk
            reset
          end
        end
      end
    end
  end
end
