require 'singleton'
require 'forwardable'
require 'location'

class Infraction
   ParseError = Class.new(StandardError)

   class RaiseOnErrorHandler
      include Singleton

      def handle_parse_error(data)
         raise ParseError, "invalid infraction data: #{data.inspect}"
      end
   end

   extend Forwardable

   def_delegators :@data, :number, :description, :province

   # initializes a new infraction from the given data
   def self.new_from_csv(data, *args)
      instance = new(*args)
      instance.csv_data = data
      instance
   end

   def initialize(error_handler: RaiseOnErrorHandler.instance)
      @error_handler = error_handler
   end

   def code
      data.code.to_i
   end

   def fine
      data.fine.to_i
   end

   def csv_data=(data)
      error_handler.handle_parse_error(data) unless data.length == Data.members.length

      @data = Data.new(*data)
   end

   def location
      Location.new(*data.values_at(6..9))
   end

private
   attr_reader :error_handler, :data

   # Column 01: First three (3) characters masked with asterisks
   # Column 02: Date the infraction occurred in YYYYMMDD format
   # Column 03: Applicable Infraction code (numeric)
   # Column 04: Short description of the infraction
   # Column 05: Amount of set fine applicable (in dollars)
   # Column 06: Time the infraction occurred  in HHMM format (24-hr clock)
   # Column 07: Code to denote proximity (see table below)
   #  AT  - At
   #  NR  - Near
   #  OPP - Opposite
   #  R/O - Rear of
   #  N/S - North side
   #  S/S - South side
   #  E/S - East side
   #  W/S - West side
   #  N/O - North of
   #  S/O - South of
   #  E/O - East of
   #  W/O - West of
   # Column 08: Street address
   # Column 09: Code to denote proximity (see location_1 table) [OPTIONAL]
   # Column 10: Street address [OPTIONAL]
   # Column 11: Province or state code of vehicle licence plate
   Data = Struct.new(:number, :date, :code, :description, :fine, :time, :location_1, :location_2, :location_3, :location_4, :province)
end