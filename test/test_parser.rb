# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'rubygems'

require 'mocha'
require 'shoulda'

require 'weari'

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
    setup do
      @arc_list = ["CDL-1.arc.gz", "CDL-2.warc.gz"]
      @ganapati = mock()
      @pig = mock()
      @pig_job = mock()
      @parser = Weari::Parser.new(:ganapati => @ganapati, :pig => @pig, :weari_java_home => "/tmp/weari_java")
      # stub out a bunch of stuff by default
      @ganapati.stubs(:exists?).returns(false)
      @ganapati.stubs(:put)
      @ganapati.stubs(:rm)
      @ganapati.stubs(:mkdir)
      @ganapati.stubs(:ls).returns([])
      @pig_job.stubs(:run).returns(true)
      @pig_job.stubs(:<<)
      @pig.stubs(:new_job).returns(@pig_job)
    end
    
    should "create an arc list" do
      @ganapati.unstub(:put)
      @ganapati.expects(:put).with do |source, target|
        # Check generated arc list
        assert_equal(@arc_list.join("\n") + "\n", File.open(source).read())
        true
      end
      
      # parse ARCS
      @parser.parse_arcs(@arc_list)
    end

    should "not reparse an existing arc" do
      @ganapati.unstub(:exists?)
      @ganapati.expects(:exists?).with("json/#{@arc_list[0]}.json").returns(false)
      @ganapati.expects(:exists?).with("json/#{@arc_list[1]}.json").returns(true)

      @ganapati.unstub(:put)
      @ganapati.expects(:put).with do |source, target|
        # Check generated arc list
        assert_equal(@arc_list[0] + "\n", File.open(source).read())
        true
      end
      
      # parse ARCS
      @parser.parse_arcs(@arc_list)
    end
    
    should "reparse until it works" do
      @arcs = ["CDL-1.arc.gz",
               "CDL-2.warc.gz",
               "CDL-3.arc.gz",
               "CDL-4.arc.gz",
               "CDL-5.arc.gz" ]
      # we expect first to try all 5 ARCS
      @parser.expects(:_parse_arcs).with(@arcs).returns(false)
      # this failed, so we divide in half
      @parser.expects(:_parse_arcs).with(@arcs.take(2)).returns(false)
      @parser.expects(:_parse_arcs).with(@arcs.drop(2)).returns(true)
      # the first group failed (CDL-1, CDL-2) so we expect it to split
      # that group in half
      @parser.expects(:_parse_arcs).with([@arcs[0]]).returns(true)
      @parser.expects(:_parse_arcs).with([@arcs[1]]).returns(false)

      assert_equal([@arcs[1]], @parser.parse_arcs(@arcs))
    end
  end
end
