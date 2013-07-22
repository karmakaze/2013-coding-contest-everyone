require 'runner'

class MapReduceRunner < Runner

   attr_reader :result

   def initialize(*args)
      @result = nil
      super
   end

   def map(&block)
      @map = block
   end

   def reduce(&block)
      @reduce = block
   end

protected

   def produce(*args)
      @result = @reduce.call(@result, @map.call(*args))
   end

end