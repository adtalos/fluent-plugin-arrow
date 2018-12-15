module Fluent
  module Plugin
    class Buffer
      module ArrowBufferStringBuilder

        private

        def each_record(&block)
          raise NotImplementedError
        end

        def build_arrow_buffer_string
          count = 0
          each_record do |record|
            count += 1
            record.each do |k, v|
              @field_wrappers[k].append(v)
            end
          end
          arrow_buf = ::Arrow::ResizableBuffer.new(bytesize * 1.2)

          ::Arrow::BufferOutputStream.open(arrow_buf) do |output|
            if @format == :parquet
              Parquet::ArrowFileWriter.open(@schema, output) do |writer|
                columns = @schema.fields.map do |f|
                  ::Arrow::Column.new(f, @field_wrappers[f.name].finish)
                end
                table = ::Arrow::Table.new(@schema, columns)
                writer.write_table(table, @chunk_size)
              end
            else
              ::Arrow::RecordBatchFileWriter.open(output, @schema) do |writer|
                record_batch = ::Arrow::RecordBatch.new(@schema, count, @field_wrappers.values.map(&:finish))
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
