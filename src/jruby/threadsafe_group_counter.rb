
class ThreadsafeGroupCounter
   include Enumerable

   attr_reader :map

   def initialize(*args)
      @map = java.util.concurrent.ConcurrentHashMap.new(*args)
   end


   def [](key)
      default = java.util.concurrent.atomic.AtomicLong.new(0)

      val = @map.put_if_absent(key, default)

      if val == nil
         default.get()
      else
         val.get()
      end
   end

   def add(key, amount)
      long = java.util.concurrent.atomic.AtomicLong.new(amount)

      val = @map.put_if_absent(key, long)

      if val == nil
         long.get()
      else
         val.add_and_get(amount)
      end
   end

   def each
      @map.map {|k,v| yield k, v.get() }
   end

end