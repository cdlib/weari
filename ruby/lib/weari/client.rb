require 'weari/thrift/server'

module Weari
  class Client
    def initialize(host, port)
      @t_transport = ::Thrift::BufferedTransport.new(::Thrift::Socket.new(host, port))
      @t_protocol = ::Thrift::BinaryProtocol.new(@t_transport)
      @t_client = Weari::Thrift::Server::Client.new(@t_protocol)
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
    
    def index(arcs, extra_id, extra_fields)
      with_open_transport do
        @t_client.index(arcs, extra_id, extra_fields)
      end
    end

    def remove(arcs)
      with_open_transport do
        @t_client.remove(arcs)
      end
    end

    def set_fields(query, fields)
      with_open_transport do
        @t_client.setFields(query, fields)
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
  end
end
