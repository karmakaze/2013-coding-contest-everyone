require File.expand_path('spec_helper', File.dirname(__FILE__))

require 'synonym_list'

describe SynonymList do
   subject { SynonymList }

   let(:data) { subject.new("STREET", "ST") }

   describe "#==" do
      it "equals a string that is contained within the list" do
         data.must_be :==, "ST"
         data.must_be :==, "STREET"
      end

      it "does not equal a string that is not in the list" do
         data.wont_be :==, "AVENUE"
      end

      it "equals the same synonym list" do
         data.must_be :==, subject.new("STREET", "ST")
      end
   end
end