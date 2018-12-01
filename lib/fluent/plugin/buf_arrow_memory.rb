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
require 'fluent/plugin/buffer'
require 'fluent/plugin/buffer/arrow_memory_chunk'
require 'fluent/plugin/arrow/field_wrapper'

module Fluent
  module Plugin
    class ArrowMemoryBuffer < Fluent::Plugin::Buffer
      Plugin.register_buffer('arrow_memory', self)

      config_param :schema, :array
      config_param :arrow_format, :enum, list: [:arrow, :parquet], default: :arrow
      config_param :row_group_chunk_size, :integer, default: 1024

      attr_reader :arrow_schema

      def configure(conf)
        super

        # [{"name" => foo1, "type" => "uint64"}, {"name" => foo2, "type" => "struct", "fields" => [{"name" => bar1, "type" => "string"}]}
        @field_wrappers = @schema.each_with_object({}) do |field, h|
          h[field["name"]] = Fluent::Plugin::Arrow::FieldWrapper.build(field)
        end

        @arrow_schema = ::Arrow::Schema.new(@field_wrappers.values.map(&:arrow_field))
      end

      def resume
        return {}, []
      end

      def generate_chunk(metadata)
        Fluent::Plugin::Buffer::ArrowMemoryChunk.new(metadata, @arrow_schema, @field_wrappers, chunk_size: @row_group_chunk_size, format: @arrow_format)
      end
    end
  end
end
