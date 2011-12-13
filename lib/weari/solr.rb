# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'rubygems'

require 'ganapati'
require 'rsolr'

module Weari
  class Solr < RSolr::Client
    def self.connect(opts)
      return Weari::Solr.new(RSolr::Connection.new, opts)
    end

    def initialize(client, opts)
      super(client, opts)
      @hdfs_thrift = opts[:hdfs]
    end

    def ganapati
      @ganapati ||= Ganapati::Client.new(*(@hdfs_thrift.split(':')))
    end

    def add_fields(xml, name, values)
      values = [values] unless values.is_a? Array
      values.each do |value|
        xml.mk('field', :name => name) do
          value = strip_control(value) if (value.instance_of? String)
          xml.cdata(value)
        end
      end
    end

    def json2solr(docs, extra_fields={})
      docs = [docs] unless docs.is_a?(Array)
      client = self
      return my_build do |xml|
        xml.mk('add', {}) do
          docs.each do |doc|
            xml.mk('doc', {}) do
              doc.each do |k, v|
                name, values = case k
                              when "suppliedContentType"
                                ["mediatypesup", "#{v['top']}/#{v['sub']}"]
                              when "detectedContentType"
                                ["mediatypedet", "#{v['top']}/#{v['sub']}"]
                              when "outlinks"
                                ["outlinks", v]
                              when "filename"
                                ["arcname", v]
                              when "length"
                                ["contentlength", v]
                              when "date"
                                # convert from epoch
                                ["date", Time.at(v/1000).utc.strftime("%Y-%m-%dT%H:%M:%SZ")]
                              else
                                [k, v]
                              end
                next if name.nil?
                client.add_fields(xml, name, values)
              end
              extra_fields.each do |name, values|
                client.add_fields(xml, name, values)
              end
            end
          end
        end
      end
    end

    def my_build &block
      b = Builder.new(:encoding => 'UTF-8')
      if block_given? then yield(b) 
      else b end
      return b.to_xml
    end

    # Add the docs to the solr index.
    def add(docs)
      data = json2solr(docs)
      self.post('update', opts.merge(:data => data, 
                                     :headers => {'Content-Type' => 'text/xml'}))
    end
    
    # Index some JSON files.
    def index(arc_names, extra_fields={})
      arc_names.each do |arc_name|
        fd = ganapati.open("json/#{arc_name}.json.gz")
        #fd = open(json_file)
        if (json_file.match(/\.gz$/)) then
          fd = Yajl::Gzip::StreamReader.new(fd)
        end
        parser = Yajl::Parser.new
        json = parser.parse(fd)
        fd.close()
        
        json.each_slice(20) do |slice|
          add(slice)
        end
        begin
          commit()
        rescue
        end
      end
    end
  end
end

