# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'rubygems'

require 'fakeweb'
require 'shoulda'
require 'weari'

class TestSolrMerger < Test::Unit::TestCase
  context "solr client record merging" do
    should "throw an error when merging un-mergeable records" do
      a = {"id" => "a"}
      b = {"id" => "b"}
      assert_raise(Weari::Solr::RecordMergeException) do
        Weari::Solr::Merger.merge_records(a,b)
      end
    end
    
    should "merge sucessfully" do
      now = Time.new
      nowp = Time.new
      a = {
        "id"            => "a",
        "content"       => "foo",
        "arcname"       => "ARC-A.arc.gz",
        "job"           => "JOB-A",
        "specification" => "SPEC-A",
        "date"          => now
      }
      b = {
        "id"            => "a",
        "content"       => "foo",
        "arcname"       => "ARC-B.arc.gz",
        "job"           => "JOB-B",
        "specification" => "SPEC-B",
        "date"          => nowp
      }
      c = Weari::Solr::Merger.merge_records(a,b)
      assert_equal(["ARC-A.arc.gz", "ARC-B.arc.gz"], c["arcname"])
      assert_equal(["JOB-A", "JOB-B"], c["job"])
      assert_equal(["SPEC-A", "SPEC-B"], c["specification"])
      assert_equal([now, nowp], c["date"])
      assert_equal("foo", c["content"])
    end
  end
end
