# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'tempfile'

module Weari  
  # Class representing a pig job.
  class PigJob
    def initialize(pig, opts={})
      @pig = pig
      @script = opts[:script] || ""
      @jars = opts[:jars] || []
    end

    def register_jar(jar)
      @jars.push(jar)
    end

    def register_jars(jars)
      @jars = @jars + jars
    end

    def <<(line)
      @script << "#{line}\n"
    end

    def run
      script_file = Tempfile.new("indexer-pig")
      @jars.each do |jar|
        script_file << "REGISTER '#{jar}'\n"
      end
      script_file << @script
      script_file.close
      retval = @pig.run_job(script_file.path)
      script_file.unlink
      return retval
    end
  end
end
