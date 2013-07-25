require File.expand_path('perf_helper', File.dirname(__FILE__))
require 'benchmark'

Benchmark.bmbm do |x|
   x.report("each_line") do
      f = File.open(Datafile, "r")
      
      f.each_line do
         # no-op
      end

      f.close
   end

   x.report("chunked(512K)") do
      f = File.open(Datafile, "r")
      f.read(512 * 1024) until f.eof?
      f.close
   end

   x.report("chunked(1MB)") do
      f = File.open(Datafile, "r")
      f.read(1024 * 1024) until f.eof?
      f.close
   end

   x.report("chunked(5MB)") do
      f = File.open(Datafile, "r")
      f.read(5 * 1024 * 1024) until f.eof?
      f.close
   end
end