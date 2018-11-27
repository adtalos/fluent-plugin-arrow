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

module Fluent
  module Plugin
    class ArrowMemoryBuffer < Fluent::Plugin::Buffer
      Plugin.register_buffer('arrow_memory', self)

      config_param :schema, :array

      attr_reader :arrow_schema

      def configure(conf)
        super

        # [{"name" => foo1, "type" => "uint64"}, {"name" => foo2, "type" => "struct", "fields" => [{"name" => bar1, "type" => "string"}]}
        arrow_fields = @schema.map do |field|
          create_arrow_field(field)
        end

        @arrow_schema = Arrow::Schema.new(arrow_fields)
      end

      def resume
        return {}, []
      end

      def generate_chunk(metadata)
        Fluent::Plugin::Buffer::ArrowMemoryChunk.new(metadata, @arrow_schema)
      end

      private

      def create_arrow_field(field)
        Arrow::Field.new(field["name"], create_arrow_data_type(field))
      end

      def create_arrow_data_type(field)
        case field["type"]
        when "struct"
          Arrow::StructDataType.new(field["fields"].map { |f| create_arrow_field(f) })
        when "list"
          Arrow::ListDataType.new(create_arrow_field(field["value_type"]))
        when "timestamp"
          Arrow::TimestampDataType.new(field["unit"].to_sym)
        else
          data_type_name = field["type"].to_s.capitalize.gsub(/\AUint/, "UInt")
          data_type_class_name = "#{data_type_name}DataType"
          data_type_class = Arrow.const_get(data_type_class_name)
          data_type_class.new
        end
      end
    end
  end
end
