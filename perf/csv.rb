require File.expand_path('perf_helper', File.dirname(__FILE__))
require 'benchmark'

require 'csv'

LINE = "***68492,20120101,192,STAND SIGNED TRANSIT STOP,60,0014,W/S,PARLIAMENT ST,S/O,VERNER LANE,ON"

Benchmark.bmbm do |x|

   x.report("#parse_line") do
      100_000.times { CSV.parse_line(LINE) }
   end
   
end