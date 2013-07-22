require File.expand_path('spec_helper', File.dirname(__FILE__))

require 'address'

describe Address do
   subject { Address }

   let(:sample) { subject.new('801 KING STREET W') }

   describe "#raw" do
      it 'gives the raw string' do
         sample.raw.must_equal '801 KING STREET W'
      end
   end

   describe "#tokens" do
      it 'gives the parsed elements' do
         sample.tokens.must_equal %w(801 KING STREET W)
      end

      it 'strips out non-digits and non-letters' do
         subject.new('33$ ADE;LAIDE ROAD').tokens.must_equal %w(33 ADELAIDE ROAD)
      end

      it 'strips out non-digits and non-letters before splitting the string' do
         subject.new('$ ADELAIDE ROAD').tokens.must_equal %w(ADELAIDE ROAD)
      end
   end

   describe "#data" do
      it "parses the street number" do
         sample.data[:number].must_equal "801"
      end

      it 'parses the direction' do
         sample.data[:direction_token].must_equal "W"
         sample.data[:direction].must_be :==, "WEST"
      end

      it 'parses the suffix' do
         sample.data[:suffix_token].must_equal "STREET"
         sample.data[:suffix].must_be :==, "ST"
      end

      it 'parses the name' do
         sample.data[:street_name].must_equal "KING"
      end

      it 'joins multiple remaining tokens to form the street name' do
         subject.new('100 QUEENS QUAY AVE NORTH').data[:street_name].must_equal "QUEENS QUAY"
      end
   end

   describe "#==" do
      it 'equals another address with the same data' do
         sample.must_be :==, subject.new("801 KING STREET W")
      end

      it 'does not equal another address with a different number' do
         sample.wont_be :==, subject.new("101 KING STREET W")
      end

      it 'does not equal another address with a different direction' do
         sample.wont_be :==, subject.new("801 KING STREET E")
      end

      it 'does not equal another address with a different suffix' do
         sample.wont_be :==, subject.new("801 KING AVENUE W")
      end

      it 'does not equal another address with a different street name' do
         sample.wont_be :==, subject.new("801 QUEEN STREET W")
      end

      it 'equals another address with a semanticaly equivalent suffix' do
         sample.must_be :==, subject.new("801 KING ST W")
      end

      it 'equals another address with a semanticaly equivalent direction' do
         sample.must_be :==, subject.new("801 KING STREET WEST")
      end
   end

   describe "#number" do
      it 'returns the parsed street number' do
         sample.number.must_equal "801"
      end
   end

   describe "#street_name" do
      it 'returns the parsed street name' do
         sample.street_name.must_equal "KING"
      end
   end

   describe "#suffix" do
      it 'returns the parsed suffix' do
         sample.suffix.must_be :==, "ST"
      end
   end

   describe "#suffix_token" do
      it 'returns the parsed suffix token' do
         sample.suffix_token.must_equal "STREET"
      end
   end

   describe "#direction" do
      it 'returns the parsed direction' do
         sample.direction.must_be :==, "WEST"
      end
   end

   describe "#direction_token" do
      it 'returns the parsed direction token' do
         sample.direction_token.must_equal "W"
      end
   end
end