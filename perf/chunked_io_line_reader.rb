require File.expand_path('perf_helper', File.dirname(__FILE__))
require 'benchmark'

require 'chunked_io_line_reader'

Benchmark.bmbm do |x|

   x.report("chunked(64k)") do
      file = File.open(Datafile, "r")
      reader = ChunkedIoLineReader.new(file, buffer_size: 64 * 1024)
      reader.each {|line|}
   end

   x.report("chunked(128k)") do
      file = File.open(Datafile, "r")
      reader = ChunkedIoLineReader.new(file, buffer_size: 128 * 1024)
      reader.each {|line|}
   end

   x.report("chunked(256k)") do
      file = File.open(Datafile, "r")
      reader = ChunkedIoLineReader.new(file, buffer_size: 256 * 1024)
      reader.each {|line|}
   end

   x.report("chunked(512k)") do
      file = File.open(Datafile, "r")
      reader = ChunkedIoLineReader.new(file, buffer_size: 512 * 1024)
      reader.each {|line|}
   end

   x.report("chunked(1MB)") do
      file = File.open(Datafile, "r")
      reader = ChunkedIoLineReader.new(file, buffer_size: 1024 * 1024)
      reader.each {|line|}
   end

   x.report("chunked(5MB)") do
      file = File.open(Datafile, "r")
      reader = ChunkedIoLineReader.new(file, buffer_size: 5 * 1024 * 1024)
      reader.each {|line|}
   end
   
end