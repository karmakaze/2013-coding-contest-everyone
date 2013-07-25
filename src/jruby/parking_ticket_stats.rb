# add the source directory to the load path
$:.unshift File.expand_path(File.dirname(__FILE__))

require 'threaded_runner'
require 'threadsafe_group_counter'
require 'summarize_fines_by_street_name'

$counter = ThreadsafeGroupCounter.new

runner = ThreadedRunner.new(STDIN, SummarizeFinesByStreetName.new($counter), pool_size: 8, timeout: 1200)

runner.done do |row|
   STDERR.puts "\ndone (row=#{row})"

   open("results.txt", "w+") do |f|
      $counter.sort_by(&:last).reverse.each {|k, v| f.puts "#{k}: #{v}" }
   end
end

runner.run!

$counter.map