# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'rubygems'

require 'weari'
require 'shoulda'
require 'mocha'

class TestParser < Test::Unit::TestCase
  context "creating a parser" do
    should "fail with empty args" do
      assert_raise(ArgumentError) do
        parser = Weari::Parser.new({})
      end
    end
    should "work when we pass in pig and ganapati" do
      assert_nothing_thrown do
        ganapati = mock()
        pig = mock()
        parser = Weari::Parser.new(:ganapati => ganapati, :pig => pig, :weari_java_home => "/tmp/weari_java")
      end
    end
  end
  
  context "parsers" do
    should "create a pig job" do
      arc_list = ["CDL-1.arc.gz", "CDL-2.warc.gz"]
      pig = mock()
      ganapati = mock()
      parser = Weari::Parser.new(:ganapati => ganapati, :pig => pig, :weari_java_home => "/tmp/weari_java")
      
      ganapati.expects(:put).with do |source, target|
        # Check generated arc list
        assert_equal(arc_list.join("\n") + "\n", File.open(source).read())
        true
      end
      # stub out a bunch of stuff which we can't really test
      ganapati.stubs(:rm)
      ganapati.stubs(:mkdir)
      ganapati.stubs(:ls).returns([])
      pig_job = mock()
      pig_job.stubs(:run)
      pig_job.stubs(:<<)
      pig.expects(:new_job).returns(pig_job)
      
      # parse ARCS
      parser.parse_arcs(arc_list)
    end
  end
end
