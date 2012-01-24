# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'rubygems'

require 'fakeweb'
require 'shoulda'

require 'weari'
require 'rsolr'
require 'nokogiri'

class TestSolr < Test::Unit::TestCase
  def setup
    @rsolr = RSolr.connect(:url=>"http://localhost/solr")
    @rsolr_mock = mock()
    @rsolr_mock.stubs(:get)
    @merger_mock = mock()
    @parser = mock()
  end
  
  context "solr client" do
    should "be able to be initialized" do
      client = Weari::Solr::Indexer.new(@rsolr_mock, @parser, @merger)
    end

    should "convert json docs" do
      solr = Weari::Solr::Indexer.new(@rsolr_mock, @parser, @merger)
      
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
end
