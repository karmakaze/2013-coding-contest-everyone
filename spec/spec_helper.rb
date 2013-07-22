require 'bundler'
Bundler.require

require 'minitest/autorun'

# set up the load path for the jruby source code
$:.unshift File.expand_path("../src/jruby", File.dirname(__FILE__))
