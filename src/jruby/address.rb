require 'synonym_list'

class Address
   attr_reader :raw

   StreetNumberRegex = /^\d+$/

   NonAddressSymbols = /[^0-9A-Za-z\s]+/

   KnownSuffixes = SynonymList.from_file File.join(File.dirname(__FILE__), "address_suffixes.txt")

   KnownDirections = SynonymList.from_file File.join(File.dirname(__FILE__), "address_directions.txt")

   def initialize(raw)
      @raw = raw
   end

   def ==(rhs)
      lhs_data = data.values_at(:number, :direction, :suffix, :street_name)
      rhs_data = rhs.data.values_at(:number, :direction, :suffix, :street_name)

      lhs_data == rhs_data
   end

   def tokens
      @tokens ||= raw.gsub(NonAddressSymbols, '').split
   end

   def number
      data[:number]
   end

   def street_name
      data[:street_name]
   end

   def direction
      data[:direction]
   end

   def direction_token
      data[:direction_token]
   end

   def suffix
      data[:suffix]
   end

   def suffix_token
      data[:suffix_token]
   end

   def data
      @data ||= begin
         result = {}

         toks = tokens.dup

         if idx = toks.index {|t| t =~ StreetNumberRegex }
            result[:number] = toks.delete_at(idx)
         end

         if dir = KnownDirections[toks.last]
            result[:direction_token] = toks.delete_at(-1)
            result[:direction] = dir
         end

         if suffix = KnownSuffixes[toks.last]
            result[:suffix_token] = toks.delete_at(-1)
            result[:suffix] = suffix
         end

         result[:street_name] = toks.join(" ")

         result
      end
   end
end