require File.expand_path('perf_helper', File.dirname(__FILE__))
require 'benchmark'

require 'threaded_runner'

class Generator
   include Enumerable

   DATA = "TEST DATA".freeze

   def each
      1_000_000.times { yield DATA }
   end
end

class VoidTask
   def each(infr, row)
      #no-op
   end
end

gen = Generator.new
task = VoidTask.new

Benchmark.bmbm do |x|

   x.report("pool=2, batch=500") do
      ThreadedRunner.new(gen, task, pool_size: 2, batch_size: 500).run!
   end

   x.report("pool=4, batch=500") do
      ThreadedRunner.new(gen, task, pool_size: 4, batch_size: 500).run!
   end

   x.report("pool=8, batch=500") do
      ThreadedRunner.new(gen, task, pool_size: 8, batch_size: 500).run!
   end

   x.report("pool=16, batch=500") do
      ThreadedRunner.new(gen, task, pool_size: 8, batch_size: 500).run!
   end

   #!-----------------------------------------------------------------

   x.report("pool=2, batch=1000") do
      ThreadedRunner.new(gen, task, pool_size: 2, batch_size: 1000).run!
   end

   x.report("pool=4, batch=1000") do
      ThreadedRunner.new(gen, task, pool_size: 4, batch_size: 1000).run!
   end

   x.report("pool=8, batch=1000") do
      ThreadedRunner.new(gen, task, pool_size: 8, batch_size: 1000).run!
   end
   
   x.report("pool=16, batch=1000") do
      ThreadedRunner.new(gen, task, pool_size: 8, batch_size: 1000).run!
   end

   #!-----------------------------------------------------------------

   x.report("pool=2, batch=1500") do
      ThreadedRunner.new(gen, task, pool_size: 2, batch_size: 1500).run!
   end

   x.report("pool=4, batch=1500") do
      ThreadedRunner.new(gen, task, pool_size: 4, batch_size: 1500).run!
   end

   x.report("pool=8, batch=1500") do
      ThreadedRunner.new(gen, task, pool_size: 8, batch_size: 1500).run!
   end
   
   x.report("pool=16, batch=1500") do
      ThreadedRunner.new(gen, task, pool_size: 8, batch_size: 1500).run!
   end
end