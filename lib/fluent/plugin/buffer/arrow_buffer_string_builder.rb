module Fluent
  module Plugin
    class Buffer
      module ArrowBufferStringBuilder

        private

        def each_record(&block)
          raise NotImplementedError
        end

        def build_arrow_buffer_string
          record_batch = ::Arrow::RecordBatch.new(@schema, each_record)
          arrow_buf = ::Arrow::ResizableBuffer.new(bytesize * 1.2)
          record_batch.to_table.save(arrow_buf,
                                     format: @format,
                                     chunk_size: @chunk_size)
          arrow_buf.data
        end
      end
    end
  end
end
