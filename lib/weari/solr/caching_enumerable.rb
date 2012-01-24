require 'rubygems'


module Weari
  module Solr

    # A class that can be used to create Enumerables that pull data
    # from a cache. Classes extending this need to implement the
    # fill_cache method and fill the @cache var with something
    class CachingEnumerable
      include Enumerable

      def initialize
        @cache_pos = 0
        @cache ||= []
      end
      
      def each
        while(self.has_next)
          yield(self.next)
        end
      end
        
      def _fill_cache
        if (@cache.length <= @cache_pos) then
          @cache_pos = 0
          self.fill_cache()
        end
      end
      
      def has_next
        if (@cache.length <= @cache_pos) then
          self._fill_cache()
        end
        return (@cache.length > 0)
      end
      
      def next
        if (!self.has_next) then
          return nil
        else
          retval = @cache[@cache_pos]
          @cache_pos = @cache_pos + 1
          return retval
        end
      end
    end
  end
end
