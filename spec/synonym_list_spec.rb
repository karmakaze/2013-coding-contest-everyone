require File.expand_path('spec_helper', File.dirname(__FILE__))

require 'synonym_list'

describe SynonymList do
   subject { SynonymList }

   let(:list) { subject.new("STREET", "ST") }

   describe "==" do
      it "equals a string that is contained within the list" do
         list.must_be :==, "ST"
         list.must_be :==, "STREET"
      end

      it "does not equal a string that is not in the list" do
         list.wont_be :==, "AVENUE"
      end

      it "equals the same synonym list" do
         list.must_be :==, subject.new("STREET", "ST")
      end
   end
end