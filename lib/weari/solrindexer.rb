# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'rubygems'

require 'bloomfilter-rb'
require 'ganapati'
require 'rsolr'
require 'weari'

module Weari
  class RecordMergeException < Exception ; end
    
  class SolrIndexer
    def initialize(rsolr_client, parser, merge_query)
      @rsolr_client, @parser, @merge_query = rsolr_client, parser, merge_query
      build_bloomfilter()
    end

    MERGE_FIELDS = {
      "arcname"       => true,
      "date"          => true,
      "job"           => true,
      "specification" => true }
      
    def build_bloomfilter
      results = @rsolr_client.get('select', :params => {:q => @merge_query, :fl => "id" })
    end

    def merge_records(a, b)
      merged = {}
      a.each do |k, v|
        if MERGE_FIELDS[k] then
          merged[k] = [v]
        else
          merged[k] = v
        end
      end
      b.each do |k, v|
        if MERGE_FIELDS[k] then
          merged[k] = merged[k] + [v]
        else
          if (merged[k] != v) then
            raise RecordMergeException.new
          end
        end
      end
      return merged
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

    def json2solr(docs, extra_id, extra_fields={})
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
                               when "tags"
                                 ["tag", v]
                              else
                                [k, v]
                              end
                next if name.nil?
                client.add_fields(xml, name, values)
              end
              # we should canonicalize the URI
              client.add_fields(xml, "id", "#{doc['url']}.#{doc['digest']}.#{extra_id}")
              extra_fields.each do |name, values|
                client.add_fields(xml, name, values)
              end
            end
          end
        end
      end
    end

    def my_build &block
      b = Weari::Builder.new(:encoding => 'UTF-8')
      if block_given? then yield(b) 
      else b end
      return b.to_xml
    end

    # Add the docs to the solr index.
    def add(docs, extra_id, extra_fields={})
      data = json2solr(docs, extra_id, extra_fields)
      @rsolr_client.post('update', 
                         :data    => data, 
                         :wt      => :ruby,
                         :headers => {'Content-Type' => 'text/xml'})
    end
    
    # Index some JSON files.
    def index(arc_names, options={})
      extra_fields = options['extra_fields'] || {}
      extra_id = options['extra_id']
      dedup_by = options['dedup_by']

      # Check that all arcs are parsed
      arc_names.each do |n| 
        raise Exception if !@parser.parsed?(n)
      end

      arc_names.compact.each do |arc_name|
        json = @parser.get_json(arc_name)
        json.each_slice(20) do |slice|
          add(slice, extra_id, extra_fields)
        end
        begin
          @rsolr_client.commit()
        rescue
        end
      end
    end
  end
end

