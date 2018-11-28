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
require 'fluent/plugin/buffer/chunk'
require 'fluent/plugin/buffer/memory_chunk'

module Fluent
  module Plugin
    class Buffer
      class ArrowMemoryChunk < MemoryChunk
        def initialize(metadata, schema, chunk_size: 1024, format: :arrow)
          super(metadata, compress: :text)
          @schema = schema
          @chunk_size = chunk_size
          @format = format
          @array_builders = {}
          @schema.fields.each do |f|
            @array_builders[f.name] = field_to_array_builder(f)
          end
          @unpacker = Fluent::MessagePackFactory.engine_factory.unpacker
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

        def field_to_array_builder(f)
          data_type_str = f.data_type.to_s
          if data_type_str =~ /timestamp/
            return Arrow::TimestampArrayBuilder.new(f.data_type)
          end

          data_type_name = data_type_str.capitalize.gsub(/\AUint/, "UInt")
          array_builder_class_name = "#{data_type_name}ArrayBuilder"
          array_builder_class = Arrow.const_get(array_builder_class_name)
          if array_builder_class.method(:new).arity > 0
            array_builder_class.new(f.data_type)
          else
            array_builder_class.new
          end
        end

        def build_arrow_buffer_string
          count = 0
          @unpacker.feed_each(@chunk) do |record|
            count += 1
            record.each do |k, v|
              if v.nil?
                @array_builders[k].append_null
              else
                @array_builders[k].append(v)
              end
            end
          end
          arrow_buf = Arrow::ResizableBuffer.new(@chunk_bytes * 1.2)

          Arrow::BufferOutputStream.open(arrow_buf) do |output|
            if @format == :parquet
              Parquet::ArrowFileWriter.open(@schema, output) do |writer|
                columns = @schema.fields.map do |f|
                  Arrow::Column.new(f, @array_builders[f.name].finish)
                end
                table = Arrow::Table.new(@schema, columns)
                writer.write_table(table, @chunk_size)
              end
            else
              Arrow::RecordBatchFileWriter.open(output, @schema) do |writer|
                record_batch = Arrow::RecordBatch.new(@schema, count, @array_builders.values.map(&:finish))
                writer.write_record_batch(record_batch)
              end
            end
          end

          arrow_buf.data.to_s
        end
      end
    end
  end
end
