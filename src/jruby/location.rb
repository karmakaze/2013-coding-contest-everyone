require 'address'

class Location < Struct.new(:location_1, :location_2, :location_3, :location_4)

   def address
      if location_2
         Address.new(location_2)
      else
         #TODO: perhaps a null object instead?
         nil
      end
   end

end