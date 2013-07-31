
class SynonymList
   attr_reader :synonyms

   def self.from_file(filename)
      lists = File.read(filename)
                  .split(/\n/)
                  .reject {|line| line =~ /^#/ || line.strip.length == 0 }
                  .map {|line| new(*line.split) }

      lists.inject({}) do |hash, list|
         list.synonyms.each {|syn| hash[syn] = list }
         hash
      end
   end
   
   def initialize(*args)
      @synonyms = args
   end

   def ==(rhs)
      if rhs.is_a?(SynonymList)
         @synonyms == rhs.synonyms
      else
         @synonyms.include?(rhs)
      end
   end
end