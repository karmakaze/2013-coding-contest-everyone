
Root = File.expand_path("..", File.dirname(__FILE__))

$:.unshift File.join(Root, "src", "jruby")

Datafile = File.join(Root, "target", "test-classes", "Parking_Tags_Data_2012.csv")