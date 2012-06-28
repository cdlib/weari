require 'rsolr'
require 'weari/thrift/server'

module Weari
  class Client
    # shuffle an arraye
    def shuffle(a_orig)
      a = a_orig.dup
      n = a.size
      while (n > 1) do
        k = rand(n)
        n = n - 1
        old = a[n]
        a[n] = a[k]
        a[k] = old
      end
      return a
    end

    def initialize(host, port)
      @t_transport = ::Thrift::BufferedTransport.new(::Thrift::Socket.new(host, port))
      @t_protocol = ::Thrift::BinaryProtocol.new(@t_transport)
      @t_client = Weari::Thrift::Server::Client.new(@t_protocol)
    end

    # Return the least used solr server from a list
    def get_best_solr_server(servers)
      best_server = nil
      best_server_doccount = 0
      shuffle(servers).each do |url|
        rsolr = RSolr.connect(:url=>url)
        response = rsolr.get('select', 
                             :params=>{"q" => '*:*', "wt" => "ruby", "rows"=>0})
        doccount = response["response"]["numFound"]
        if (best_server.nil? || (doccount < best_server_doccount)) then
          best_server = url
          best_server_doccount = doccount
        end
      end
      return best_server
    end

    def with_open_transport
      @t_transport.open()
      retval = yield()
      @t_transport.close()
      return retval
    end
    
    def parse_arcs(arcs)
      with_open_transport do
        @t_client.parseArcs(arcs)
      end
    end

    def parse_arc(arc)
      return with_open_transport do
        @t_client.parseArc(arc)
      end
    end
    
    def index(solr_uri, filter, arcs, extra_id, extra_fields)
      with_open_transport do
        @t_client.index(solr_uri, filter, arcs, extra_id, extra_fields)
      end
    end

    def set_fields(solr_uri, query, fields)
      with_open_transport do
        @t_client.setFields(solr_uri, query, fields)
      end
    end

    def is_arc_parsed(arc)
      return with_open_transport do
        @t_client.isArcParsed(arc)
      end
    end

    def make_empty_json(arc)
      return with_open_transport do
        @t_client.makeEmptyJson(arc)
      end
    end

    def delete_parse(arc)
      return with_open_transport do
        @t_client.deleteParse(arc)
      end
    end

    def clear_merge_manager(manager_id)
      return with_open_transport do
        @t_client.clearMergeManager(manager_id)
      end
    end
  end
end
