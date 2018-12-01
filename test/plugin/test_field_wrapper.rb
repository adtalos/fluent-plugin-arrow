require "helper"
require "fluent/plugin/arrow/field_wrapper"

class ArrowFieldWrapperTest < Test::Unit::TestCase
  test ".build (string)" do
    field_wrapper = Fluent::Plugin::Arrow::FieldWrapper.build({"name" => "key1", "type" => "string"})
    assert_equal "key1", field_wrapper.name
    assert_equal "string", field_wrapper.type
    assert_kind_of Arrow::Field, field_wrapper.arrow_field
  end

  test ".build (timestamp)" do
    field_wrapper = Fluent::Plugin::Arrow::FieldWrapper.build({"name" => "key1", "type" => "timestamp", "unit" => "nano"})
    assert_equal "key1", field_wrapper.name
    assert_equal "timestamp", field_wrapper.type
    assert_kind_of Arrow::Field, field_wrapper.arrow_field
  end

  test ".build (list)" do
    field_wrapper = Fluent::Plugin::Arrow::FieldWrapper.build({"name" => "key1", "type" => "list", "value_type" => {"name" => "value", "type" => "string"}})
    assert_equal "key1", field_wrapper.name
    assert_equal "list", field_wrapper.type
    assert_kind_of Arrow::Field, field_wrapper.arrow_field
    assert_kind_of Arrow::ListDataType, field_wrapper.arrow_field.data_type
    assert_kind_of Arrow::ListArrayBuilder, field_wrapper.array_builder

    assert_equal "value", field_wrapper.children[0].name
    assert_equal "string", field_wrapper.children[0].type
    assert_kind_of Arrow::Field, field_wrapper.children[0].arrow_field
    assert_kind_of Arrow::StringDataType, field_wrapper.children[0].arrow_field.data_type
    assert_kind_of Arrow::StringArrayBuilder, field_wrapper.children[0].array_builder
  end

  test ".build (struct)" do
    field_wrapper = Fluent::Plugin::Arrow::FieldWrapper.build({"name" => "key1", "type" => "struct", "fields" => [
      {"name" => "foo1", "type" => "string"},
      {"name" => "foo2", "type" => "uint64"},
      {"name" => "foo3", "type" => "timestamp", "unit" => "milli"},
    ]})
    assert_equal "key1", field_wrapper.name
    assert_equal "struct", field_wrapper.type
    assert_kind_of Arrow::Field, field_wrapper.arrow_field
    assert_kind_of Arrow::StructDataType, field_wrapper.arrow_field.data_type
    assert_kind_of Arrow::StructArrayBuilder, field_wrapper.array_builder

    assert_equal "foo1", field_wrapper.children[0].name
    assert_equal "string", field_wrapper.children[0].type
    assert_kind_of Arrow::Field, field_wrapper.children[0].arrow_field
    assert_kind_of Arrow::StringDataType, field_wrapper.children[0].arrow_field.data_type
    assert_kind_of Arrow::StringArrayBuilder, field_wrapper.children[0].array_builder

    assert_equal "foo2", field_wrapper.children[1].name
    assert_equal "uint64", field_wrapper.children[1].type
    assert_kind_of Arrow::Field, field_wrapper.children[1].arrow_field
    assert_kind_of Arrow::UInt64DataType, field_wrapper.children[1].arrow_field.data_type
    assert_kind_of Arrow::UInt64ArrayBuilder, field_wrapper.children[1].array_builder

    assert_equal "foo3", field_wrapper.children[2].name
    assert_equal "timestamp", field_wrapper.children[2].type
    assert_kind_of Arrow::Field, field_wrapper.children[2].arrow_field
    assert_kind_of Arrow::TimestampDataType, field_wrapper.children[2].arrow_field.data_type
    assert_kind_of Arrow::TimestampArrayBuilder, field_wrapper.children[2].array_builder
  end

  test ".build (nested)" do
    field_wrapper = Fluent::Plugin::Arrow::FieldWrapper.build({"name" => "key1", "type" => "struct", "fields" => [
      {"name" => "foo1", "type" => "string"},
      {"name" => "foo2", "type" => "list", "value_type" => {"name" => "value", "type" => "uint64"}},
    ]})
    assert_equal "key1", field_wrapper.name
    assert_equal "struct", field_wrapper.type
    assert_kind_of Arrow::Field, field_wrapper.arrow_field
    assert_kind_of Arrow::StructDataType, field_wrapper.arrow_field.data_type
    assert_kind_of Arrow::StructArrayBuilder, field_wrapper.array_builder

    assert_equal "foo1", field_wrapper.children[0].name
    assert_equal "string", field_wrapper.children[0].type
    assert_kind_of Arrow::Field, field_wrapper.children[0].arrow_field
    assert_kind_of Arrow::StringDataType, field_wrapper.children[0].arrow_field.data_type
    assert_kind_of Arrow::StringArrayBuilder, field_wrapper.children[0].array_builder

    assert_equal "foo2", field_wrapper.children[1].name
    assert_equal "list", field_wrapper.children[1].type
    assert_kind_of Arrow::Field, field_wrapper.children[1].arrow_field
    assert_kind_of Arrow::ListDataType, field_wrapper.children[1].arrow_field.data_type
    assert_kind_of Arrow::ListArrayBuilder, field_wrapper.children[1].array_builder

    assert_equal "value", field_wrapper.children[1].children[0].name
    assert_equal "uint64", field_wrapper.children[1].children[0].type
    assert_kind_of Arrow::Field, field_wrapper.children[1].children[0].arrow_field
    assert_kind_of Arrow::UInt64DataType, field_wrapper.children[1].children[0].arrow_field.data_type
    assert_kind_of Arrow::UInt64ArrayBuilder, field_wrapper.children[1].children[0].array_builder
  end

  test "#append (timestamp)" do
    field_wrapper = Fluent::Plugin::Arrow::FieldWrapper.build({"name" => "key1", "type" => "timestamp", "unit" => "nano"})
    time = Time.now
    field_wrapper.append(time)
    timestamp_array = field_wrapper.finish
    assert_kind_of Time, timestamp_array[0]
    assert_equal time.to_i, timestamp_array[0].to_i
  end

  test "#append (date32)" do
    field_wrapper = Fluent::Plugin::Arrow::FieldWrapper.build({"name" => "key1", "type" => "date32"})
    date = Date.today
    field_wrapper.append(date)
    date_array = field_wrapper.finish
    assert_kind_of Date, date_array[0]
    assert_equal date, date_array[0]
  end

  test "#append (date64)" do
    field_wrapper = Fluent::Plugin::Arrow::FieldWrapper.build({"name" => "key1", "type" => "date64"})
    date = Date.today
    field_wrapper.append(date)
    date_array = field_wrapper.finish
    assert_kind_of DateTime, date_array[0]
    assert_equal date, date_array[0].to_date
  end

  test "#append (nested)" do
    field_wrapper = Fluent::Plugin::Arrow::FieldWrapper.build({"name" => "key1", "type" => "struct", "fields" => [
      {"name" => "foo1", "type" => "string"},
      {"name" => "foo2", "type" => "list", "value_type" => {"name" => "value", "type" => "uint64"}},
    ]})

    field_wrapper.append({"foo1" => "rec1", "foo2" => [1, 2, 3]})
    field_wrapper.append({"foo1" => "rec2", "foo2" => [4, 5]})

    struct_array = field_wrapper.finish
    assert_kind_of Arrow::StringArray, struct_array.fields[0]
    assert_equal "rec1", struct_array.fields[0][0]
    assert_equal "rec2", struct_array.fields[0][1]

    assert_kind_of Arrow::UInt64Array, struct_array.fields[1].get_value(0)
    assert_equal 1, struct_array.fields[1].get_value(0)[0]
    assert_equal 2, struct_array.fields[1].get_value(0)[1]
    assert_equal 3, struct_array.fields[1].get_value(0)[2]

    assert_kind_of Arrow::UInt64Array, struct_array.fields[1].get_value(1)
    assert_equal 4, struct_array.fields[1].get_value(1)[0]
    assert_equal 5, struct_array.fields[1].get_value(1)[1]
  end
end
