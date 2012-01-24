# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'rubygems'

require 'weari'

module Weari
  # Class for running pig jobs.
  class Pig
    def initialize(opts)
      @hadoop_home = opts[:hadoop_home] or raise ArgumentError.new("Missing :hadoop_home option!")
      @pig_jar = opts[:pig_jar] or raise ArgumentError.new("Missing :pig_jar option!")
      @pig_opts = opts[:pig_opts] or raise ArgumentError.new("Missing :pig_opts option!")
      @hadoop_bin = File.join(@hadoop_home, "bin", "hadoop")
      @hadoop_conf = File.join(@hadoop_home, "conf")
      hadoop_env = File.join(@hadoop_conf, "hadoop-env.sh")
      @java_home = `bash -c "source #{hadoop_env} && echo -n \\$JAVA_HOME"`
      @java_bin = File.join(@java_home, 'bin', 'java')
    end

    def new_job(*args)
      return Weari::PigJob.new(self, *args)
    end

    def run_job(script_path)
      system("#{@java_bin} -Xmx1024m -cp #{@hadoop_conf}:#{@pig_jar} #{@pig_opts} org.apache.pig.Main #{script_path}")
    end
  end
end
