# add the source directory to the load path
$:.unshift File.expand_path(File.dirname(__FILE__))

require 'each_runner'

$counts = Hash.new(0)

runner = EachRunner.new(STDIN)

runner.each do |infr, row|
   if infr.location.address
      $counts[infr.location.address.street_name] += infr.fine
   end
end

# runner.progress { STDERR.print "." }

runner.done do |row|
   STDERR.puts "\ndone (row=#{row})"
end

runner.run!

$counts.each do |street, value|
   result.put(street, value.to_java(Java::int))
end

open("out.txt", "w+")  {|f| f.puts result.inspect }