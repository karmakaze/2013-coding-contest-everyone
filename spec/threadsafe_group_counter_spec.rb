require File.expand_path('spec_helper', File.dirname(__FILE__))

require 'threadsafe_group_counter'

describe ThreadsafeGroupCounter do
   subject { ThreadsafeGroupCounter }

   let(:sample) { subject.new }

   it 'stores and returns a value' do
      sample.add(:foo, 5).must_equal 5
   end

   it 'retrieves the stored value' do
      sample.add(:foo, 3)
      sample[:foo].must_equal 3
   end

   it 'adds multiple calls to #add together' do
      sample.add(:foo, 3)
      sample.add(:foo, 2)

      sample[:foo].must_equal 5
   end

   it 'returns zero for an undefined key' do
      sample[:foo].must_equal 0
   end

   it 'yields pairs of <String, Fixnum> when iterating' do
      sample.add("KING", 100)
      sample.to_a.must_equal [["KING", 100]]
   end
end