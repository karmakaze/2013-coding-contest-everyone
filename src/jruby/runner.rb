require 'csv'
require 'infraction'

class Runner

   def initialize(io, error_handler: IgnoreHandler.new)
      @io = io
      @error_handler = error_handler
   end

   def run!
      # throw away the first line
      @io.readline

      row = 1

      # run through each line, parse it and pass it on to the subclass to handle it
      @io.each_line do |line|
         row += 1

         # attempt to parse the line as CSV, if we fail, run the error handler and move on
         begin
            data = CSV.parse_line(line)
         rescue CSV::MalformedCSVError => e
            error_handler.handle_malformed_csv_error(self, e, row)
            next
         end

         # if the data is nil for any reason, ignore this line
         #TODO: does this ever happen?
         next if data == nil

         #TODO: rather than generating a new infraction every time, consider replacing the data
         produce Infraction.new_from_csv(data, error_handler: error_handler), row

         @progress.call(row) if @progress
      end

      @io.close

      @done.call(row) if @done
   end

   def progress(&block)
      @progress = block
   end

   def done(&block)
      @done = block
   end

protected
   attr_reader :error_handler

   def produce(infraction, idx)
      raise NotImplementedError, "please implement me!"
   end

private

   class IgnoreHandler
      def handle_malformed_csv_error(runner, error, row)
         # no-op
      end

      def handle_parse_error(data)
         # no-op
      end
   end

end