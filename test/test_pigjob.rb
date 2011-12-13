# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'rubygems'

require 'weari'
require 'shoulda'
require 'mocha'

class TestPigJob < Test::Unit::TestCase
  context "creating a pig job" do
    should "create a script file with the right data" do
      pig = mock()
      job = Weari::PigJob.new(pig)
      job.register_jar("lib/test.jar")
      job << "LINE 1"

      # set up test
      pig.expects(:run_job).with do |script_file|
        assert_equal("REGISTER 'lib/test.jar'\nLINE 1\n", File.open(script_file).read)
        true
      end

      job.run
    end
  end
end
