namespace rb weari.thrift
namespace java org.cdlib.was.weari.thrift

exception IndexException {
  1: string why
}

service Server {
  void ping(),

  void index(1: string solr,
             2: string filter,
             3: list<string> arcs,
             4: string extraId,
             5: map<string,string> extraFields)
    throws (1: IndexException ex)
}
