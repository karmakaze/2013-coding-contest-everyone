
class SynonymList
   attr_reader :synonyms
   
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