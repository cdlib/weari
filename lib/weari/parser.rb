# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'weari/pig'
require 'ganapati'
require 'tempfile'

module Weari
  class Parser
    def initialize(opts)
      raise ArgumentError.new("Missing :hdfs_script option!") if
        (opts[:ganapati].nil? && opts[:hdfs_thrift].nil?)
      @ganapati = opts[:ganapati] ||
        Ganapati::Client.new(*(opts[:hdfs_thrift].split(':')))
      @weari_java_home = opts[:weari_java_home] or       
        raise ArgumentError.new("Missing :weari_java_home option!")
      if opts[:pig].nil? then
        raise ArgumentError.new("Missing :hadoop_home option!") if
          opts[:hadoop_home].nil?
      end
      @pig = opts[:pig] || Weari::Pig.new(:pig_jar     => File.join(@weari_java_home, "pig.jar"),
                                          :hadoop_home => opts[:hadoop_home],
                                          :pig_opts    => "-Dpig.splitCombination=false")
    end

    def refile_json(source)
      @ganapati.mkdir("json")
      @ganapati.ls(source).each do |path|
        if path.match(/\.json\.gz$/) then
          name = path.split(/\//)[-1]
          @ganapati.mv(path, "json/#{name}")
        else
          if (@ganapati.stat(path).isdir)
            refile_json(path)
          end
        end
        @ganapati.rm(path)
      end
      @ganapati.rm(source)
    end

    def mk_arc_list(arcs)
      arclist = Tempfile.new("arclist")
      arcs.each do |arc|
        arclist << "#{arc}\n"
      end
      arclist.close
      arclist_hdfs = File.basename(arclist.path)
      
      # move arclist to HDFS
      @ganapati.put(arclist.path, arclist_hdfs)
      arclist.unlink
      return arclist_hdfs
    end

    def parse_arcs(arcs)
      # Where to put the output
      outputdir = (0...10).map{ ('a'..'z').to_a[rand(26)] }.join 

      # generate list of arcs
      arclist_hdfs = mk_arc_list(arcs)

      # build pig script
      pig_job = @pig.new_job(:jars => Dir[File.join(@weari_java_home, "lib", "*")])
      pig_job << "Data = LOAD '#{arclist_hdfs}' USING org.cdlib.was.ngIndexer.pig.ArchiveURLParserLoader() AS (filename:chararray, url:chararray, digest:chararray, date:chararray, length:long, content:chararray, detectedMediaType:chararray, suppliedMediaType:chararray, title:chararray, outlinks);"
      pig_job << "STORE Data INTO '#{outputdir}.json.gz' USING org.cdlib.was.ngIndexer.pig.JsonParsedArchiveRecordStorer();"
      
      pig_job.run
      # remove arclist
      @ganapati.rm(arclist_hdfs)
      refile_json("#{outputdir}.json.gz")
    end
  end
end
