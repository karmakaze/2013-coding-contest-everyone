require File.expand_path('perf_helper', File.dirname(__FILE__))
require 'benchmark'

require 'process_infraction_task'

class VoidTask
   def each(infr, row)
      #no-op
   end
end

TASK = VoidTask.new

LINE = "***68492,20120101,192,STAND SIGNED TRANSIT STOP,60,0014,W/S,PARLIAMENT ST,S/O,VERNER LANE,ON"

Benchmark.bmbm do |x|

   x.report("#new") do
      10_000.times { ProcessInfractionTask.new(LINE, TASK, 0) }
   end

   x.report("#run") do
      task = ProcessInfractionTask.new(LINE, TASK, 0)
      10_000.times { task.run }
   end
   
end