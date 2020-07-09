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

require "arrow"
require "parquet"
require 'fluent/plugin/buffer'
require 'fluent/plugin/buffer/arrow_memory_chunk'

module Fluent
  module Plugin
    class ArrowMemoryBuffer < Fluent::Plugin::Buffer
      Plugin.register_buffer('arrow_memory', self)

      config_param :schema, :array
      config_param :arrow_format, :enum, list: [:arrow, :parquet], default: :arrow
      config_param :codec, :enum, list: [:text, :gzip, :brotli, :snappy, :lz4, :zstd], default: :text
      config_param :chunk_size_per_row_group, :integer, default: 64*1024*1024
      config_param :desired_buffer_size_in_bytes, :integer, default: 1<<30

      attr_reader :arrow_schema

      def configure(conf)
        super

        # [{"name" => foo1, "type" => "uint64"}, {"name" => foo2, "type" => "struct", "fields" => [{"name" => bar1, "type" => "string"}]}
        @arrow_schema = ::Arrow::Schema.new(@schema)
      end

      def resume
        return {}, []
      end

      def generate_chunk(metadata)
        Fluent::Plugin::Buffer::ArrowMemoryChunk.new(metadata, @arrow_schema, chunk_size_per_row_group: @chunk_size_per_row_group, desired_buffer_size_in_bytes: @desired_buffer_size_in_bytes, format: @arrow_format, codec: @codec, compress: :text, log: log)
        
      end
    end
  end
end
