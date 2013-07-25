require 'process_infraction_task'

class ThreadedRunner

   def initialize(io, task, error_handler: IgnoreHandler.new, pool_size: 4, timeout: 600)
      @io = io
      @task = task
      @pool_size = pool_size
      @timeout = timeout
      @error_handler = error_handler
   end

   def run!
      start = Time.now.to_f

      # throw away the first line
      @io.readline

      row = 1

      # run through each line, parse it and pass it on to the subclass to handle it
      @io.each_line do |line|
         row += 1

         submit line, row

         @progress.call(row) if @progress
      end

      @io.close

      final = Time.now.to_f

      STDOUT.puts "IO loop completed in #{final - start} seconds"

      shutdown!

      STDOUT.puts "Queue shut down after another #{Time.now.to_f - final} seconds"

      @done.call(row) if @done
   end

   def progress(&block)
      @progress = block
   end

   def done(&block)
      @done = block
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

   attr_reader :pool_size, :timeout, :error_handler

   def pool
      @pool ||= java.util.concurrent.Executors.newFixedThreadPool(pool_size)
   end

   def submit(line, idx)
      pool.submit ProcessInfractionTask.new(line, @task, idx)
   end

   # stop accepting submissions and wait up to 10 seconds for the worker
   # queue to finish what it has left.
   def shutdown!
      pool.shutdown

      pool.await_termination timeout, java.util.concurrent.TimeUnit::SECONDS
   end

end