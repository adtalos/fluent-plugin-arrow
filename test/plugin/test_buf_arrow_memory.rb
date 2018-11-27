require "helper"
require 'fluent/plugin/buf_arrow_memory'
require 'fluent/plugin/output'

module FluentPluginArrowMemoryBufferTest
  class DummyOutputPlugin < Fluent::Plugin::Output
  end
end

class ArrowMemoryBufferTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
    @d = FluentPluginArrowMemoryBufferTest::DummyOutputPlugin.new
    @p = Fluent::Plugin::ArrowMemoryBuffer.new
    @p.owner = @d
  end

  test 'this is non persistent plugin' do
    assert !@p.persistent?
  end

  test 'configure' do
    conf = %[
      schema [
        {"name": "foo1", "type": "uint64"},
        {"name": "foo2", "type": "string"},
        {"name": "foo3", "type": "timestamp", "unit": "milli"},
        {"name": "foo4", "type": "list", "value_type": {"name": "value", "type": "uint64"}},
        {"name": "foo5", "type": "struct", "fields": [{"name": "bar1", "type": "uint64"}, {"name": "bar2", "type": "list", "value_type": {"name": "value", "type": "string"}}]}
      ]
    ]
    buffer_conf = Fluent::Config.parse(conf, "(test)", "(test_dir)", syntax: :v1)
    @p.configure(buffer_conf)
    assert @p.arrow_schema.is_a?(Arrow::Schema)
    assert @p.arrow_schema.n_fields == 5
    assert @p.arrow_schema.fields[0].data_type.is_a?(Arrow::UInt64DataType)
    assert @p.arrow_schema.fields[1].data_type.is_a?(Arrow::StringDataType)
    assert @p.arrow_schema.fields[2].data_type.is_a?(Arrow::TimestampDataType)
    assert @p.arrow_schema.fields[3].data_type.is_a?(Arrow::ListDataType)
    assert @p.arrow_schema.fields[3].data_type.value_field.data_type.is_a?(Arrow::UInt64DataType)
    assert @p.arrow_schema.fields[4].data_type.is_a?(Arrow::StructDataType)
  end

  test 'generate_chunk' do
    conf = %[
      schema [
        {"name": "foo1", "type": "uint64"},
        {"name": "foo2", "type": "string"}
      ]
    ]
    buffer_conf = Fluent::Config.parse(conf, "(test)", "(test_dir)", syntax: :v1)
    @p.configure(buffer_conf)
    chunk = @p.generate_chunk(Object.new)
    assert chunk.is_a?(Fluent::Plugin::Buffer::ArrowMemoryChunk)
  end
end
