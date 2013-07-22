require File.expand_path('spec_helper', File.dirname(__FILE__))

require 'infraction'

describe Infraction do
   subject { Infraction }

   let(:data) { ['***68492','20120101','192','STAND SIGNED TRANSIT STOP','60','0014','W/S','PARLIAMENT ST','S/O','VERNER LANE','ON'] }
   let(:sample) { subject.new_from_csv(data) }

   it 'raises an error when given an insufficient amount of data' do
      -> { subject.new.csv_data = [] }.must_raise Infraction::ParseError
   end

   it 'parses the masked tag number' do
      sample.number.must_equal "***68492"
   end

   it 'parses the code' do
      sample.code.must_equal 192
   end

   it 'parses the description' do
      sample.description.must_equal "STAND SIGNED TRANSIT STOP"
   end

   it 'parses the fine' do
      sample.fine.must_equal 60.0
   end

   it 'parses the province' do
      sample.province.must_equal "ON"
   end

   describe "#location" do
      it 'is a location object' do
         sample.location.must_be_kind_of(Location)
      end

      it 'has all location data passed to it' do
         sample.location.location_1.must_equal "W/S"
         sample.location.location_2.must_equal "PARLIAMENT ST"
         sample.location.location_3.must_equal "S/O"
         sample.location.location_4.must_equal "VERNER LANE"
      end
   end

end