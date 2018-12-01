require "arrow"

module Fluent
  module Plugin
    module Arrow
      class FieldWrapper
        class << self
          def build(field)
            case field["type"]
            when "string"
              StringFieldWrapper.new(field)
            when "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64"
              IntegerFieldWrapper.new(field)
            when "float", "double"
              FloatFieldWrapper.new(field)
            when "boolean"
              BooleanFieldWrapper.new(field)
            when "date32"
              Date32FieldWrapper.new(field)
            when "date64"
              Date64FieldWrapper.new(field)
            when "timestamp"
              TimestampFieldWrapper.new(field)
            when "list"
              ListFieldWrapper.new(field)
            when "struct"
              StructFieldWrapper.new(field)
            else
              raise "Unsupported data type"
            end
          end
        end

        attr_reader :field, :name, :type, :children, :arrow_field, :array_builder

        def initialize(field)
          @field = field
          @name = field["name"]
          @type = field["type"]
          @children = []

          field["value_type"]&.tap do |f|
            @children << self.class.build(f)
          end

          field["fields"]&.each do |f|
            @children << self.class.build(f)
          end

          create_arrow_field
          create_array_builder
        end

        def append(value)
          if value.nil?
            @array_builder.append_null
          else
            @array_builder.append(cast_value(value))
          end
        end

        def finish
          @array_builder.finish
        end

        def create_arrow_field
          @arrow_field = ::Arrow::Field.new(name, create_arrow_data_type)
        end

        def create_arrow_data_type
          data_type_name = type.to_s.capitalize.gsub(/\AUint/, "UInt")
          data_type_class_name = "#{data_type_name}DataType"
          data_type_class = ::Arrow.const_get(data_type_class_name)
          data_type_class.new
        end

        def create_array_builder(from_parent = nil)
          if from_parent
            @array_builder = from_parent
          else
            data_type_str = arrow_field.data_type.to_s
            data_type_name = data_type_str.capitalize.gsub(/\AUint/, "UInt")
            array_builder_class_name = "#{data_type_name}ArrayBuilder"
            array_builder_class = ::Arrow.const_get(array_builder_class_name)
            @array_builder = array_builder_class.new
          end
        end

        def cast_value(value)
          raise NotImplementedError
        end
      end

      class StringFieldWrapper < FieldWrapper
        def cast_value(value)
          value.to_s
        end
      end

      class IntegerFieldWrapper < FieldWrapper
        def cast_value(value)
          value.to_i
        end
      end

      class FloatFieldWrapper < FieldWrapper
        def cast_value(value)
          value.to_f
        end
      end

      class BooleanFieldWrapper < FieldWrapper
        def cast_value(value)
          !!value
        end
      end

      require "date"
      class Date32FieldWrapper < FieldWrapper
        UNIX_EPOCH = Date.new(1970, 1, 1)
        def cast_value(value)
          date =
            if value.respond_to?(:to_date)
              value.to_date
            else
              Date.parse(value)
            end

          (date - UNIX_EPOCH).to_i
        end

        def create_array_builder(from_parent = nil)
          if from_parent
            @array_builder = from_parent
          else
            @array_builder = ::Arrow::Date32ArrayBuilder.new
          end
        end
      end

      class Date64FieldWrapper < FieldWrapper
        UNIX_EPOCH = Date.new(1970, 1, 1)
        def cast_value(value)
          time =
            if value.respond_to?(:to_time)
              value.to_time
            else
              Time.parse(value)
            end

          time.to_i * 1_000 + time.usec / 1_000
        end

        def create_array_builder(from_parent = nil)
          if from_parent
            @array_builder = from_parent
          else
            @array_builder = ::Arrow::Date64ArrayBuilder.new
          end
        end
      end

      require "time"
      class TimestampFieldWrapper < FieldWrapper
        def cast_value(value)
          value =
            if value.is_a?(Fluent::EventTime)
              Time.at(value, value.usec)
            elsif value.respond_to?(:to_time)
              value.to_time
            elsif value.is_a?(String)
              Time.parse(value)
            else
              value
            end

          return value if value.is_a?(Numeric)

          case field["unit"]
          when "second"
            value.to_i
          when "milli"
            value.to_i * 1_000 + value.usec / 1_000
          when "micro"
            value.to_i * 1_000_000 + value.usec
          else
            value.to_i * 1_000_000_000 + value.nsec
          end
        end

        def create_arrow_data_type
          ::Arrow::TimestampDataType.new(field["unit"].to_sym)
        end

        def create_array_builder(from_parent = nil)
          if from_parent
            @array_builder = from_parent
          else
            @array_builder = ::Arrow::TimestampArrayBuilder.new(arrow_field.data_type)
          end
        end
      end

      class ListFieldWrapper < FieldWrapper
        def append(value)
          if value.nil?
            @array_builder.append_null
          else
            @array_builder.append
            value.each do |v|
              @children[0].append(v)
            end
          end
        end

        def create_arrow_data_type
          ::Arrow::ListDataType.new(children[0].arrow_field)
        end

        def create_array_builder(from_parent = nil)
          if from_parent
            @array_builder = from_parent
          else
            @array_builder = ::Arrow::ListArrayBuilder.new(arrow_field.data_type)
          end

          @children.each { |c| c.create_array_builder(@array_builder.value_builder) }
        end
      end

      class StructFieldWrapper < FieldWrapper
        def append(value)
          if value.nil?
            @array_builder.append_null
          else
            @array_builder.append
            value.each do |k, v|
              @children.find { |c| c.name == k }.append(v)
            end
          end
        end

        def create_arrow_data_type
          ::Arrow::StructDataType.new(children.map(&:arrow_field))
        end

        def create_array_builder(from_parent = nil)
          if from_parent
            @array_builder = from_parent
          else
            @array_builder = ::Arrow::StructArrayBuilder.new(arrow_field.data_type)
          end

          @children.each_with_index { |c, i| c.create_array_builder(@array_builder.get_field_builder(i)) }
        end
      end
    end
  end
end
