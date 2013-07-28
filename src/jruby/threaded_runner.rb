require 'process_infraction_task'

class ThreadedRunner

   GroupCount = 1000

   def initialize(reader, task, pool_size: 4, timeout: 600, error_handler: IgnoreHandler.new)
      @reader = reader
      @task = task
      @pool_size = pool_size
      @timeout = timeout
      @error_handler = error_handler
   end

   def run!
      start = Time.now.to_f

      tasks = Array.new(GroupCount)
      idx = 0

      @reader.each_with_index do |line, row|
         tasks[idx] = ProcessInfractionTask.new(line, @task, row)

         if idx == GroupCount - 1
            emit(tasks)
            idx = 0
         else
            idx += 1
         end
      end

      emit(tasks) if tasks.any?

      final = Time.now.to_f

      STDOUT.puts "IO loop completed in #{final - start} seconds"

      shutdown!

      STDOUT.puts "Queue shut down after another #{Time.now.to_f - final} seconds"
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
         @tasks.each {|t| t.run if t }
      end
   end

   attr_reader :pool_size, :timeout, :error_handler

   def pool
      @pool ||= java.util.concurrent.Executors.newFixedThreadPool(pool_size)
   end

   def emit(tasks)
      pool.submit TaskGroupHandler.new(tasks.dup)
   end

   # stop accepting submissions and wait up to 10 seconds for the worker
   # queue to finish what it has left.
   def shutdown!
      pool.shutdown

      pool.await_termination timeout, java.util.concurrent.TimeUnit::SECONDS
   end

end