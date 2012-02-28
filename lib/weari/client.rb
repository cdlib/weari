require 'weari/thrift/server'

module Weari
  class Client
    def initialize(host, port)
      @t_transport = ::Thrift::BufferedTransport.new(::Thrift::Socket.new(host, port))
      @t_protocol = ::Thrift::BinaryProtocol.new(@t_transport)
      @t_client = Weari::Thrift::Server::Client.new(@t_protocol)
    end

    def parse_arcs(arcs)
      @t_transport.open()
      @t_client.parseArcs(arcs)
      @t_transport.close()
    end
    
    def index(solr_uri, filter, arcs, extra_id, extra_fields)
      @t_transport.open()
      @t_client.index(solr_uri, filter, arcs, extra_id, extra_fields)
    end
  end
end
