# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'rubygems'

require 'fakeweb'
require 'shoulda'
require 'weari'
require 'weari/solrindexer'
require 'rsolr'
require 'nokogiri'

class TestSolr < Test::Unit::TestCase
  def setup
    @merge_query = "*:*"
    @rsolr = RSolr.connect(:url=>"http://localhost/solr")
    @rsolr_mock = mock()
    @rsolr_mock.stubs(:get)
    @parser = mock()
    @merge_query = "*:*"
  end
  
  context "solr client" do
    should "be able to be initialized" do
      client = Weari::SolrIndexer.new(@rsolr_mock, @parser, @merge_query)
    end

    should "convert json docs" do
      solr = Weari::SolrIndexer.new(@rsolr_mock, @parser, @merge_query)
      
      json_doc = {
        "url" => "http://example.org/",
        "suppliedContentType" => { "top" => "application",
          "sub" => "xml+xhtml" },
        "detectedContentType" => { "top" => "text",
          "sub" => "html" }
      }
      doc = Nokogiri::XML(solr.json2solr(json_doc, { "foo" => "bar" }))
      assert_equal("http://example.org/",
                   doc.xpath('/add/doc/field[@name="url"]').text)
      assert_equal("text/html",
                   doc.xpath('/add/doc/field[@name="mediatypedet"]').text)
      assert_equal("application/xml+xhtml",
                   doc.xpath('/add/doc/field[@name="mediatypesup"]').text)
    end
  end
  
  context "solr client record merging" do
    should "throw an error when merging un-mergeable records" do
      client = Weari::SolrIndexer.new(@rsolr_mock, @parser, @merge_query)
      a = {"id" => "a"}
      b = {"id" => "b"}
      assert_raise(Weari::RecordMergeException) do
        client.merge_records(a,b)
      end
    end
    
    should "merge sucessfully" do
      client = Weari::SolrIndexer.new(@rsolr_mock, @parser, @merge_query)
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
      c = client.merge_records(a,b)
      assert_equal(["ARC-A.arc.gz", "ARC-B.arc.gz"], c["arcname"])
      assert_equal(["JOB-A", "JOB-B"], c["job"])
      assert_equal(["SPEC-A", "SPEC-B"], c["specification"])
      assert_equal([now, nowp], c["date"])
      assert_equal("foo", c["content"])
    end
  end
end
