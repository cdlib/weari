package org.cdlib.was.ngIndexer.akka;

import java.util.Date;

import org.apache.solr.common.SolrInputDocument;

import org.cdlib.was.ngIndexer._;

sealed trait IndexMessage;

case class IndexArc(url : String)
  extends IndexMessage;

case class Parse(bytes : Array[Byte], record : IndexArchiveRecord)
  extends IndexMessage;

case class MakeSolrDocument(record : IndexArchiveRecord,
                            result : MyParseResult)
  extends IndexMessage;

case object ParseError extends IndexMessage;

case class IndexDocument(doc : SolrInputDocument);

case object MakeDocumentError extends IndexMessage;
