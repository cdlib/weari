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
      @pig_job.stubs(:run)
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
      @ganapati.expects(:exists?).with("json/#{@arc_list[0]}.json.gz").returns(false)
      @ganapati.expects(:exists?).with("json/#{@arc_list[1]}.json.gz").returns(true)

      @ganapati.unstub(:put)
      @ganapati.expects(:put).with do |source, target|
        # Check generated arc list
        assert_equal(@arc_list[0] + "\n", File.open(source).read())
        true
      end
      
      # parse ARCS
      @parser.parse_arcs(@arc_list)
    end
  end
end
