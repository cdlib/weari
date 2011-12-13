# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'rubygems'

require 'fakeweb'
require 'shoulda'
require 'weari'

class TestSolr < Test::Unit::TestCase
  context "solr client" do
    should "be able to be initialized" do
      client = Weari::Solr.connect(:url  => "http://localhost:8983/solr",
                                   :hdfs => "localhost:52563")
    end

    should "convert json docs" do
      solr = Weari::Solr.connect(:url  => "http://example.org:8983/",
                                 :hdfs => "localhost:52563")
      json_doc = {
        "url" => "http://example.org/",
        "suppliedContentType" => { "top" => "text",
          "sub" => "xml+xhtml" },
        "detectedContentType" => { "top" => "text",
          "sub" => "html" }

      }
      xml_data = <<EOXML
<?xml version="1.0" encoding="UTF-8"?>
<add>
  <doc>
    <field name="mediatypesup"><![CDATA[text/xml+xhtml]]></field>
    <field name="url"><![CDATA[http://example.org/]]></field>
    <field name="mediatypedet"><![CDATA[text/html]]></field>
    <field name="foo"><![CDATA[bar]]></field>
  </doc>
</add>
EOXML
      assert_equal(xml_data, solr.json2solr(json_doc, { "foo" => "bar" }))
    end
    
    should "commit" do
      solr = Weari::Solr.connect(:url => "http://localhost/solr")
      FakeWeb.register_uri(:post, /http:\/\/localhost\/solr\/update/,
                           :content_type => "text/plain; charset=UTF-8",
                           :body => "{'responseHeader'=>{'status'=>0,'QTime'=>12}}")
      solr.commit
    end
  end
end
