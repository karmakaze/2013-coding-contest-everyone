require File.expand_path('spec_helper', File.dirname(__FILE__))

require 'chunked_io_line_reader'
require 'stringio'

describe ChunkedIoLineReader do
   subject { ChunkedIoLineReader }

   let(:io) { StringIO.new("foo\nbar\nbaz") }
   let(:dos_io) { StringIO.new("foo\r\nbar\r\nbaz") }

   describe "#initialize" do
      it 'accepts an io object' do
         subject.new(io).io.must_equal io
      end

      it 'has a default buffer size of 512k' do
         subject.new(io).buffer_size.must_equal 512 * 1024
      end

      it 'allows overriding the buffer size' do
         subject.new(io, buffer_size: 128).buffer_size.must_equal 128
      end
   end

   describe "#each" do
      let(:reader) { subject.new(io, buffer_size: 1) }

      it "yields all lines of the IO" do
         reader.to_a.must_equal %w(foo bar baz)
      end

      it "yields lines from a DOS file (CRLF line endings)" do
         subject.new(dos_io).to_a.must_equal %w(foo bar baz)
      end
   end

end