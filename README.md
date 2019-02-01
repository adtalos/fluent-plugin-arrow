# fluent-plugin-arrow

[Fluentd](https://fluentd.org/) buffer plugin to output Apache Arrow and Parquet format.

## Prerequisite

- [Apache Arrow c++](https://github.com/apache/arrow/tree/master/cpp) (with -DARROW_PARQUET=ON)
- [Apache Arrow c_glib](https://github.com/apache/arrow/tree/master/c_glib)
- [red-arrow](https://github.com/apache/arrow/tree/master/ruby/red-arrow)
- [red-parquet](https://github.com/apache/arrow/tree/master/ruby/red-parquet)

## Installation

### RubyGems

```
$ gem install fluent-plugin-arrow
```

### Bundler

Add following line to your Gemfile:

```ruby
gem "fluent-plugin-arrow"
```

And then execute:

```
$ bundle
```

## Configuration

You can generate configuration template:

```
<match arrow>
  @type file

  path arrow_test

  <buffer>
    @type arrow_memory
    arrow_format arrow # or parquet

    schema [
      {"name": "key1", "type": "string"},
      {"name": "key2", "type": "uint64"},
      {"name": "key3", "type": "timestamp", "unit": "milli"},
      {"name": "key4", "type": "list", "field": {"name": "value", "type": "uint64"}},
      {"name": "key5", "type": "struct", "fields": [
        {"name": "bar1", "type": "uint64"},
        {"name": "bar2", "type": "list", "field": {"name": "value", "type": "string"}}
      ]}
    ]
  </buffer>

  <format>
    @type arrow
  </format>
</match>
```

You can copy and paste generated documents here.

## Copyright

* Copyright(c) 2018- joker1007
* License
  * Apache License, Version 2.0
