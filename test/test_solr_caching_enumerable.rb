# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'rubygems'

require 'weari/solr/caching_enumerable'
require 'shoulda'

class TestSolrCachingEnumerable < Test::Unit::TestCase
  context "caching enumerable" do
    should "work" do
      class Foo < Weari::Solr::CachingEnumerable
        def initialize()
          @done = false
          super()
        end

        def fill_cache
          if @done then
            @cache = []
          else
            @cache = [1,2,3]
            @done = true
          end
        end
      end
      f = Foo.new
      assert_equal([2,4,6], f.map { |x| x * 2 })
    end
  end
end
