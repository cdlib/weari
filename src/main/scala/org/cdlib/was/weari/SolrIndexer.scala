/* Copyright (c) 2009-2012 The Regents of the University of California */

package org.cdlib.was.weari;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.{SolrDocument,SolrInputDocument};
import org.cdlib.was.weari.SolrFields._;
import org.cdlib.was.weari.SolrDocumentModifier.addFields;

import grizzled.slf4j.Logging;

/**
 * Class used to index ARC files.
 */
class SolrIndexer (server : SolrServer, 
                   manager : MergeManager,
                   extraId : String, 
                   extraFields : Map[String, Any]) 
    extends Logging {

  /**
   * Convert a ParsedArchiveRecord into a SolrInputDocument, merging
   * in extraFields and extraId (see SolrIndexer constructor).
   */
  def record2inputDocument (record : ParsedArchiveRecord) : SolrInputDocument = {
    val doc = ParsedArchiveRecordSolrizer.convert(record);
    addFields(doc, extraFields.toSeq : _*);
    doc.setField(ID_FIELD, "%s.%s".format(getId(doc), extraId));
    return doc;
  }

  /**
   * Index a sequence of ParsedArchiveRecords.
   * Existing documents will be merged with new documents.
   */
  def index (recs : Seq[ParsedArchiveRecord]) {
    for (rec <- recs) {
      server.add(manager.merge(record2inputDocument(rec)));
    }
  }

  /**
   * Perform f, and either commit at the end if there were no exceptions,
   * or rollback if there were.
   */
  def commitOrRollback[A] (f: => A) : A = {
    try {
      val retval = f;
      server.commit;
      return retval;
    } catch {
      case ex : Exception => {
        server.rollback;
        throw ex;
      }
    }
  }
}
