package org.cdlib.was.ngIndexer.pig;

import org.apache.pig.{Algebraic,EvalFunc};
import org.apache.pig.data.{DataBag,DataType,Tuple};
import org.apache.pig.data.TupleFactory;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;

import org.cdlib.was.ngIndexer._;

import scala.collection.JavaConversions.collectionAsScalaIterable;

class SolrIndex extends EvalFunc[String] with Algebraic {
  def exec (input : Tuple) : String = {
    SolrIndex.indexTuple(input);
    val solrUrl = input.get(0).asInstanceOf[String];
    val server = new StreamingUpdateSolrServer(solrUrl,
                                               SolrIndex.config.queueSize(),
                                               SolrIndex.config.threadCount());
    server.commit;
    return "";
  }

  def getInitial = classOf[SolrIndexInitial].getName;

  def getIntermed = classOf[SolrIndexIntermed].getName;

  def getFinal = classOf[SolrIndexFinal].getName;
}

object SolrIndex {
  val config = new Config {};
  val indexer = new SolrIndexer(config);
  
  def indexTuple (input : Tuple) : String = {
    val solrUrl = input.get(0).asInstanceOf[String];
    if (!solrUrl.startsWith("http://localhost"))
      throw new RuntimeException(input.toString);
    val server = new StreamingUpdateSolrServer(solrUrl,
                                               config.queueSize(),
                                               config.threadCount());
    val filter = new QuickIdFilter ("*:*", server, 1000);
    val keys = input.get(1).asInstanceOf[Tuple].getAll.asInstanceOf[java.util.List[String]];
    val values = input.get(2).asInstanceOf[Tuple].getAll.asInstanceOf[java.util.List[_]];
    val doc = new SolrInputDocument;
    for ((k, v) <- keys.zip(values)) {
      DataType.findType(v) match {
        case DataType.NULL => ();
        case DataType.BAG  => {
          val tupleIter = (v.asInstanceOf[DataBag]).iterator;
          while(tupleIter.hasNext) {
            doc.addField(k, tupleIter.next);
          }
        }
        case DataType.BYTE => {
          doc.addField(k, v);
        }
        case DataType.CHARARRAY => {
          doc.addField(k, v.toString);
        }
        case _ => throw new RuntimeException(DataType.findType(v).toString);
      }
      server.add(doc);
    }    
    return solrUrl;
  }
  val tupleFactory = TupleFactory.getInstance();

  def mkTuple (s : String) : Tuple = {
    var tuple = tupleFactory.newTupleNoCopy(new java.util.ArrayList[java.lang.Object]());
    tuple.append(s);
    return tuple;
  }
}

class SolrIndexInitial extends EvalFunc[Tuple] {
  def exec (input : Tuple) : Tuple = {
    return SolrIndex.mkTuple(SolrIndex.indexTuple(input));
  }
}

class SolrIndexIntermed extends EvalFunc[Tuple] {
  def exec (input : Tuple) : Tuple = {
    return input;
  }
}

class SolrIndexFinal extends EvalFunc[String] {
  def exec (input : Tuple) : String = {
    val solrUrl = input.get(0).asInstanceOf[String];
    if (!solrUrl.startsWith("http://localhost"))
      throw new RuntimeException(input.toString);
    val server = new StreamingUpdateSolrServer(solrUrl,
                                               SolrIndex.config.queueSize(),
                                               SolrIndex.config.threadCount());
      server.commit;
    return "";
  }
}
