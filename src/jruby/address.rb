require 'synonym_list'

class Address
   attr_reader :raw

   StreetNumberRegex = /^\d+$/

   Symbols = /[^0-9A-Za-z\s]+/

   # The first set is from http://bitsandpieces.us/2010/05/02/official-list-of-street-suffix-abbreviations/
   #TODO: move this into a datafile
   KnownSuffixes = [
      SynonymList.new("ALY", "ALLEY"),
      SynonymList.new("ANX", "ANNEX"),
      SynonymList.new("ARC", "ARCADE"),
      SynonymList.new("AV", "AVE", "AVENUE"),
      SynonymList.new("BCH", "BEACH"),
      SynonymList.new("BG", "BURG"),
      SynonymList.new("BL"),
      SynonymList.new("BLF","BLUFF"),
      SynonymList.new("BLVD", "BOULEVARD"),
      SynonymList.new("BND", "BEND"),
      SynonymList.new("BR", "BRANCH"),
      SynonymList.new("BRG", "BRIDGE"),
      SynonymList.new("BRK", "BROOK"),
      SynonymList.new("BTM", "BOTTOM"),
      SynonymList.new("BYU", "BAYOO"),
      SynonymList.new("CIR", "CIRCLE", "CRCL"),
      SynonymList.new("CLB", "CLUB"),
      SynonymList.new("CLF", "CLIFF"),
      SynonymList.new("CMN", "COMMON"),
      SynonymList.new("COR", "CORNER"),
      SynonymList.new("CP", "CAMP"),
      SynonymList.new("CPE", "CAPE"),
      SynonymList.new("CRCT", "CIRCUIT"),
      SynonymList.new("CR", "CRES", "CRESCENT"),
      SynonymList.new("CRK", "CREEK"),
      SynonymList.new("CRSE", "COURSE"),
      SynonymList.new("CRST", "CREST"),
      SynonymList.new("CSWY", "CAUSEWAY"),
      SynonymList.new("CT", "CRT", "COURT"),
      SynonymList.new("CTR", "CENTER"),
      SynonymList.new("CURV", "CURVE"),
      SynonymList.new("CV", "COVE"),
      SynonymList.new("CYN", "CANYON"),
      SynonymList.new("DL", "DALE"),
      SynonymList.new("DM", "DAM"),
      SynonymList.new("DR", "DRIVE"),
      SynonymList.new("DV", "DIVIDE"),
      SynonymList.new("EST", "ESTATE"),
      SynonymList.new("EXPY", "EXPRESSWAY"),
      SynonymList.new("EXT", "EXTENSION"),
      SynonymList.new("FALL"),
      SynonymList.new("FLD", "FIELD"),
      SynonymList.new("FLT", "FLAT"),
      SynonymList.new("FRD", "FORD"),
      SynonymList.new("FRG", "FORGE"),
      SynonymList.new("FRK", "FORK"),
      SynonymList.new("FRST", "FOREST"),
      SynonymList.new("FRY", "FERRY"),
      SynonymList.new("FT", "FORT"),
      SynonymList.new("FWY", "FREEWAY"),
      SynonymList.new("GDN", "GDNS", "GARDEN"),
      SynonymList.new("GLN", "GLEN"),
      SynonymList.new("GRN", "GREEN"),
      SynonymList.new("GRV", "GROVE"),
      SynonymList.new("GT", "GATE"),
      SynonymList.new("GTWY", "GATEWAY"),
      SynonymList.new("HBR", "HARBOR"),
      SynonymList.new("HL", "HILL"),
      SynonymList.new("HOLW", "HOLLOW"),
      SynonymList.new("HTS", "HEIGHTS"),
      SynonymList.new("HVN", "HAVEN"),
      SynonymList.new("HWY", "HIGHWAY"),
      SynonymList.new("INLT", "INLET"),
      SynonymList.new("IS", "ISLAND"),
      SynonymList.new("ISLE"),
      SynonymList.new("JCT", "JUNCTION"),
      SynonymList.new("KNL", "KNOLL"),
      SynonymList.new("KY", "KEY"),
      SynonymList.new("LAND"),
      SynonymList.new("LCK", "LOCK"),
      SynonymList.new("LDG", "LODGE"),
      SynonymList.new("LF", "LOAF"),
      SynonymList.new("LGT", "LIGHT"),
      SynonymList.new("LK", "LAKE"),
      SynonymList.new("LN", "LANE"),
      SynonymList.new("LNDG", "LANDING"),
      SynonymList.new("LOOP"),
      SynonymList.new("MALL"),
      SynonymList.new("MDW", "MEADOW"),
      SynonymList.new("ML", "MILL"),
      SynonymList.new("MNR", "MANOR"),
      SynonymList.new("MSN", "MISSION"),
      SynonymList.new("MT", "MOUNT"),
      SynonymList.new("MTN", "MOUNTAIN"),
      SynonymList.new("MTWY", "MOTORWAY"),
      SynonymList.new("NCK", "NECK"),
      SynonymList.new("ORCH", "ORCHARD"),
      SynonymList.new("OVAL"),
      SynonymList.new("PK", "PARK"),
      SynonymList.new("PATH"),
      SynonymList.new("PIKE"),
      SynonymList.new("PKWY", "PARKWAY"),
      SynonymList.new("PL", "PLACE"),
      SynonymList.new("PLN", "PLAIN"),
      SynonymList.new("PLZ", "PLAZA"),
      SynonymList.new("PNE", "PINE"),
      SynonymList.new("PR", "PRAIRIE"),
      SynonymList.new("PRT", "PORT"),
      SynonymList.new("PSGE", "PASSAGE"),
      SynonymList.new("PT", "POINT"),
      SynonymList.new("RADL", "RADIAL"),
      SynonymList.new("RAMP"),
      SynonymList.new("RD", "ROAD"),
      SynonymList.new("RDG", "RIDGE"),
      SynonymList.new("RIV", "RIVER"),
      SynonymList.new("RNCH", "RANCH"),
      SynonymList.new("ROW", "ROW"),
      SynonymList.new("RPD", "RAPID"),
      SynonymList.new("RST", "REST"),
      SynonymList.new("RTE", "ROUTE"),
      SynonymList.new("RUE"),
      SynonymList.new("RUN"),
      SynonymList.new("SHL", "SHOAL"),
      SynonymList.new("SHR", "SHORE"),
      SynonymList.new("SKWY", "SKYWAY"),
      SynonymList.new("SMT", "SUMMIT"),
      SynonymList.new("SPG", "SPRING"),
      SynonymList.new("SPUR"),
      SynonymList.new("SQ", "SQUARE"),
      SynonymList.new("ST", "STREET"),
      SynonymList.new("STA", "STATION"),
      SynonymList.new("STRA", "STRAVENUE"),
      SynonymList.new("STRM", "STREAM"),
      SynonymList.new("TER", "TERR", "TERRACE"),
      SynonymList.new("TPKE", "TURNPIKE"),
      SynonymList.new("TRAK", "TRACK"),
      SynonymList.new("TRCE", "TRACE"),
      SynonymList.new("TRFY", "TRAFFICWAY"),
      SynonymList.new("TR", "TRL", "TRAIL"),
      SynonymList.new("TRWY", "THROUGHWAY"),
      SynonymList.new("TUNL", "TUNNEL"),
      SynonymList.new("UN", "UNION"),
      SynonymList.new("VIA", "VIADUCT"),
      SynonymList.new("VIS", "VISTA"),
      SynonymList.new("VL", "VILLE"),
      SynonymList.new("VLG", "VILLAGE"),
      SynonymList.new("VLY", "VALLEY"),
      SynonymList.new("VW", "VIEW"),
      SynonymList.new("WALK"),
      SynonymList.new("WALL"),
      SynonymList.new("WAY"),
      SynonymList.new("WL", "WELL"),
      SynonymList.new("XING", "CROSSING"),
      SynonymList.new("XRD", "CROSSROAD")
   ]

   # concat rules:
   # MC + anything = MCanything
   # LAKE SHORE = LAKESHORE
   # DE + anything = Deanything

   KnownDirections = [
      SynonymList.new("N", "NORTH"),
      SynonymList.new("S", "SOUTH"),
      SynonymList.new("E", "EAST"),
      SynonymList.new("W", "WEST")
   ]

   def initialize(raw)
      @raw = raw
   end

   def ==(rhs)
      lhs_data = data.values_at(:number, :direction, :suffix, :street_name)
      rhs_data = rhs.data.values_at(:number, :direction, :suffix, :street_name)

      lhs_data == rhs_data
   end

   def tokens
      @tokens ||= raw.gsub(Symbols, '').split
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

         if idx = KnownDirections.index(toks.last)
            result[:direction_token] = toks.delete_at(-1)
            result[:direction] = dir = KnownDirections[idx]
         end

         if idx = KnownSuffixes.index(toks.last)
            result[:suffix_token] = toks.delete_at(-1)
            result[:suffix] = KnownSuffixes[idx]
         end

         result[:street_name] = toks.join(" ")

         result
      end
   end
end