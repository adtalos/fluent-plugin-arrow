require "helper"
require "fluent/plugin/formatter_arrow.rb"

class ArrowFormatterTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  private

  def create_driver(conf)
    Fluent::Test::Driver::Formatter.new(Fluent::Plugin::ArrowFormatter).configure(conf)
  end
end
