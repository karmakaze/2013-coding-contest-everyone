require 'runner'

class EachRunner < Runner

   def each(&block)
      @each = block
   end

protected

   def produce(*args)
      @each.call(*args)
   end

end