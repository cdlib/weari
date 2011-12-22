# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'weari/pig'
require 'ganapati'
require 'tempfile'

module Weari
  class Parser
    def initialize(opts)
      @group_size = opts[:group_size] || 100
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

    def reparse_arcs(arcs)
      arcs.each do |arcname|
        @ganapati.rm(json_path(arcname)) if parsed?(arcname)
      end
      parse_arcs(arcs)
    end

    def mk_arcname(arc_uri)
      return arc_uri.to_s.split(/\//)[-1]
    end
    
    def mk_json_path(arc)
      return "json/#{mk_arcname(arc)}.json.gz"
    end
    
    def parsed?(arc)
      @ganapati.exists?(mk_json_path(arc))
    end
    
    def parse_arcs_retry(arcs)
      if (self._parse_arcs(arcs)) then
        # success!
        return []
      else
        if (arcs.size < 2) then
          # bad ARC
          return arcs
        else
          # split in 2, append the results
          i = arcs.size/2
          return parse_arcs_retry(arcs.take(i)) + 
            parse_arcs_retry(arcs.drop(i))
        end
      end
    end

    def parse_arcs(arcs)
      failed_arcs = []
      # remove already parsed arcs
      arcs = arcs.delete_if { |arc| parsed?(arc) }
      # group into manageable size
      arcs.each_slice(@group_size) do |arc_slice|
        failed_arcs = failed_arcs + parse_arcs_retry(arc_slice)
      end
      return failed_arcs
    end
    
    # raw parse arcs
    def _parse_arcs(arcs)
      # Where to put the output
      outputdir = (0...10).map{ ('a'..'z').to_a[rand(26)] }.join 
      
      # generate list of arcs
      arclist_hdfs = mk_arc_list(arcs)
      
      # build pig script
      pig_job = @pig.new_job(:jars => Dir[File.join(@weari_java_home, "lib", "*")])
      pig_job << "Data = LOAD '#{arclist_hdfs}' \
          USING org.cdlib.weari.pig.ArchiveURLParserLoader() \
          AS (filename:chararray, url:chararray, digest:chararray, date:chararray, length:long, content:chararray, detectedMediaType:chararray, suppliedMediaType:chararray, title:chararray, outlinks);"
      pig_job << "STORE Data INTO '#{outputdir}.json.gz' \
        USING org.cdlib.was.weari.pig.JsonParsedArchiveRecordStorer();"
      
      success = pig_job.run
      refile_json("#{outputdir}.json.gz") if success
      @ganapati.rm(arclist_hdfs)
      return success
    end
  end
end
