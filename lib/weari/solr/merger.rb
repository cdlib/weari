# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011-2012, Regents of the University of California

require 'rubygems'

require 'bloomfilter-rb'
require 'weari'

module Weari
  module Solr

    class RecordMergeException < Exception ; end
    
    class Merger
      BF_N   = 1000000 # bloomfilter capcity
      BF_ERR = 0.001   # bloomfilter error rate

      MERGE_FIELDS = {
        "arcname"       => true,
        "date"          => true,
        "job"           => true,
        "specification" => true }

      def initialize(rsolr_client, merge_query)
        @rsolr_client, @merge_query = rsolr_client, merge_query
      end
      
      # Merge with upstream record *if necessary*. Otherwise returns
      # original record.
      def maybe_merge(record)
        if !@bf.has?(doc["id"]) then
          return record
        else
          merge_record = get_merge_record(record)
          if !merge_record then
            # got a false positive from bloomfilter, no merge needed
            return record
          else
            return Solr::Merger.merge_fields(record, merge_record)
          end
        end
      end

      # Build bloomfilter arguments for constants BF_N and BF_ERR
      def self.bf_args
        m = (BF_N * Math.log(BF_ERR) / Math.log(1.0 / 2 ** Math.log(2))).ceil
        k = (Math.log(2) * m / BF_N).round
        return {:size=>m, :hashes=>k, :seed => 101, :bucket=>2}
      end
      
      # Build a bloomfilter for quick lookup if a document with a given
      # ID is already in the index.
      def build_bloomfilter
        @bf = BloomFilter::Native.new(Weari::Solr::Merger.bf_args)
        docs = Weari::Solr::DocumentEnumerable.new(@rsolr_client, 
                                                   { :q  => @merge_query,
                                                     :fl => "id" })
        docs.each do |d|
          @bf.put(d["id"])
        end
      end
      
      # Merge two records. Throws exception if the records should not be
      # merged, e.g. they have two different IDs.
      def self.merge_records(a, b)
        merged = {}
        a.each do |k, v|
          if MERGE_FIELDS[k] then
            v = [v] if (v.class != Array)
          end
          merged[k] = v
        end
        b.each do |k, v|
          if MERGE_FIELDS[k] then
            v = [v] if (v.class != Array)
            if !merged[k].member?(v[0]) then
              merged[k] = merged[k] + v
            end
          else
            raise RecordMergeException.new if (merged[k] != v)
          end
        end
        return merged
      end
    end
  end
end
