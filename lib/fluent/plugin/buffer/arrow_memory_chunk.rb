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
require 'fluent/plugin/buffer/memory_chunk'
require 'fluent/plugin/buffer/arrow_buffer_string_builder'

module Fluent
  module Plugin
    class Buffer
      class ArrowMemoryChunk < MemoryChunk
        include ArrowBufferStringBuilder

        def initialize(metadata, schema, chunk_size: 1024, format: :arrow)
          super(metadata, compress: :text)
          @schema = schema
          @chunk_size = chunk_size
          @format = format
        end

        def read(**kwargs)
          build_arrow_buffer_string
        end

        def open(**kwargs, &block)
          StringIO.open(build_arrow_buffer_string, &block)
        end

        def write_to(io, **kwargs)
          # re-implementation to optimize not to create StringIO
          io.write build_arrow_buffer_string
        end

        private

        def each_record(&block)
          Fluent::MessagePackFactory.engine_factory.unpacker.feed_each(@chunk, &block)
        end
      end
    end
  end
end
