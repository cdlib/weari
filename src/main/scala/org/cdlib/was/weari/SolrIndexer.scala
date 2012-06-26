/*
Copyright (c) 2009-2012, The Regents of the University of California

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

* Neither the name of the University of California nor the names of
its contributors may be used to endorse or promote products derived
from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package org.cdlib.was.weari;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.{SolrDocument,SolrInputDocument};
import org.cdlib.was.weari.SolrFields._;
import org.cdlib.was.weari.SolrDocumentModifier.addFields;

import grizzled.slf4j.Logging;

/**
 * Class used to index ARC files.
 */
class SolrIndexer (config : Config,
                   server : SolrServer, 
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
    val docs = for (rec <- recs) 
               yield record2inputDocument(rec);
    /* group documents for batch merge */
    /* this will ensure that we don't build up a lot of merges before hitting the */
    /* trackCommitThreshold */
    for { group <- docs.grouped(config.batchMergeGroupSize);
          merged <- manager.batchMerge(group) } {
      server.add(merged); 
    }
  }
}
