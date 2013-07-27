
class ChunkedIoLineReader
   include Enumerable

   attr_reader :io, :buffer_size

   def initialize(io, buffer_size: 512 * 1024)
      @io = io
      @buffer_size = buffer_size
   end

   def each
      buffer = nil
      eol = nil
      
      until @io.eof?
         buffer ||= ''
         buffer << @io.read(@buffer_size)

         eol ||= buffer.index(CR) ? CRLF : LF

         p0 = 0

         while p1 = buffer.index(eol, p0)
            yield buffer[p0..p1-1]
            p0 = p1 + eol.length
         end

         # slice off everything that was already read
         buffer = buffer[p0..-1]
      end

      # yield any remaining buffer
      yield buffer if buffer.length > 0
   end

private
   CR = "\r".freeze
   LF = "\n".freeze
   CRLF = "\r\n".freeze

end