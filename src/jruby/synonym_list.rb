
class SynonymList
   attr_reader :synonyms

   def self.from_file(filename)
      File.read(filename).split(/\n/).reject {|line| line =~ /^#/ || line.strip.length == 0 } .map {|line| new(*line.split) }
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