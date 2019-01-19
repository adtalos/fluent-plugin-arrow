
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
require 'fluent/plugin/buf_file'
require 'fluent/plugin/buffer/arrow_file_chunk'

module Fluent
  module Plugin
    class ArrowFileBuffer < Fluent::Plugin::FileBuffer
      Plugin.register_buffer('arrow_file', self)

      config_param :schema, :array
      config_param :arrow_format, :enum, list: [:arrow, :parquet], default: :arrow
      config_param :row_group_chunk_size, :integer, default: 1024

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
        # FileChunk generates real path with unique_id
        if @file_permission
          chunk = Fluent::Plugin::Buffer::ArrowFileChunk.new(metadata, @path, :create, @arrow_schema, perm: @file_permission, chunk_size: @row_group_chunk_size, format: @arrow_format)
        else
          chunk = Fluent::Plugin::Buffer::ArrowFileChunk.new(metadata, @path, :create, @arrow_schema, chunk_size: @row_group_chunk_size, format: @arrow_format)
        end

        log.debug "Created new chunk", chunk_id: dump_unique_id_hex(chunk.unique_id), metadata: metadata

        return chunk
      end
    end
  end
end
