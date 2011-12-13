# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'rubygems'

require 'weari'
require 'shoulda'

class TestBuilder < Test::Unit::TestCase
  context "building xml" do
    should "be able to use mk method" do
      builder = Weari::Builder.new
      builder.mk("root") do
        builder.mk("child") do
          builder.text("text")
        end
      end

      xml_data = "<?xml version=\"1.0\"?>
<root>
  <child>text</child>
</root>
"
      assert_equal(xml_data, builder.to_xml)
    end
  end
end
