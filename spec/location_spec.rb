require File.expand_path('spec_helper', File.dirname(__FILE__))

require 'location'

describe Location do
   subject { Location }

   let(:data) { ['W/S','PARLIAMENT ST','S/O','VERNER LANE'] }
   let(:sample) { subject.new(*data) }

   describe "#address" do
      it 'is an address object' do
         sample.address.must_be_kind_of Address
      end

      it 'passes in the location data as a string' do
         sample.address.raw.must_equal "PARLIAMENT ST"
      end

      it 'is nil if there is no location_2 given' do
         subject.new([nil,nil,nil,nil]).address.must_be_nil
      end
   end
end