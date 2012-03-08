/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import org.apache.solr.client.solrj.{SolrQuery,SolrServer};

import org.archive.util.BloomFilter64bit;

/**
 * A class that wraps a bloom filter s that we can quickly look up
 * whether or not an item with a given ID is in the index without
 * needing to query each and every item.
 *
 * @author Erik Hetzner <erik.hetzner@ucop.edu>
 */
class QuickIdFilter (q : String , server : SolrServer, n : Int) {
  def this (q : String, server : SolrServer) = 
    this(q, server, 100000);

  val bf = new BloomFilter64bit(n, 12);

  if (server != null) {
    val newq = new SolrQuery(q).setParam("fl", "id").setRows(1000);
    val docs = new solr.SolrDocumentCollection(server, newq);
    for (doc <- docs) 
      bf.add(doc.get("id").asInstanceOf[String]);
  }

  def contains (s : String) = bf.contains(s);

  def add (s : String) = bf.add(s);
}
