require 'process_infraction_task'

class ThreadedRunner

   def initialize(io, task, error_handler: IgnoreHandler.new, pool_size: 4, timeout: 600, buffer_size: 512 * 1024)
      @io = io
      @task = task
      @pool_size = pool_size
      @timeout = timeout
      @error_handler = error_handler
      @buffer_size = buffer_size
   end

   def run!
      start = Time.now.to_f

      # throw away the first line
      @io.readline

      row = 1

      # run through each line, parse it and pass it on to the subclass to handle it
      buffer = ''

      until @io.eof?
         buffer << @io.read(@buffer_size)

         p0 = 0
         p1 = 0

         tasks = []

         while p1 = buffer.index("\r\n", p0)
            line = buffer[p0..p1-1]

            p0 = p1 + 2

            row += 1

            tasks << ProcessInfractionTask.new(line, @task, row)

            # puts line.inspect

            # @progress.call(row) if @progress
         end

         emit(tasks)

         # slice off everything that was already read
         puts "slice: 0..#{p0}"
         #buffer.slice!(0..p0)
         buffer = buffer[p0..-1]
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

   class TaskGroupHandler
      include java.lang.Runnable

      def initialize(tasks)
         @tasks = tasks
      end

      def run
         @tasks.each(&:run)
      end
   end

   attr_reader :pool_size, :timeout, :error_handler

   def pool
      @pool ||= java.util.concurrent.Executors.newFixedThreadPool(pool_size)
   end

   def emit(tasks)
      pool.submit TaskGroupHandler.new(tasks)
   end

   # stop accepting submissions and wait up to 10 seconds for the worker
   # queue to finish what it has left.
   def shutdown!
      pool.shutdown

      pool.await_termination timeout, java.util.concurrent.TimeUnit::SECONDS
   end

end