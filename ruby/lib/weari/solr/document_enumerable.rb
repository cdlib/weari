require 'rubygems'

require 'weari'

module Weari
  module Solr
    # A class which can be used to enumerate through Solr docs based
    # on a query against a rsolr server
    class DocumentEnumerable < Weari::Solr::CachingEnumerable
      def initialize(rsolr, params)
        @rsolr, @params = rsolr, params
        @pos = 0
        @rows = 50
        super()
      end
      
      def length
        @length ||= (@rsolr.get("select", 
                                :params=>@params.merge(:rows=>0))['response']['numFound'])
      end

      def fill_cache
        results = @rsolr.get("select", :params=>@params.merge({:rows=>@rows, :start=>@pos}))
        @cache = results['response']['docs']
        @pos = results['response']['start'] + @cache.length
      end
    end
  end
end
