require File.expand_path('perf_helper', File.dirname(__FILE__))
require 'benchmark'

require 'address'

Benchmark.bmbm do |x|
   x.report("#new") do
      1_000_000.times { Address.new("300 KING STREET W") }
   end

   x.report("#new + #tokens") do
      1_000_000.times { Address.new("300 KING STREET W").tokens }
   end

   x.report("#new + #data") do
      1_000_000.times { Address.new("300 KING STREET W").data }
   end
end