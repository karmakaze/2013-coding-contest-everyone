
class SummarizeFinesByStreetName

   def initialize(counts)
      @counts = counts
   end

   def each(infr, row_idx)
      if infr.location.address
         @counts.add(infr.location.address.street_name, infr.fine)
      else
         @counts.add("undefined", infr.fine)
      end
   end

end