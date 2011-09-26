/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer.webgraph;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.webgraph.NodeIterator;
import java.io.InputStream;

public class MyImmutableSequentialGraph extends ImmutableSequentialGraph {

    private WebGraph webGraph;

    public String toString () {
        return this.webGraph.toString();
    }

    public MyImmutableSequentialGraph(WebGraph webGraph) {
        this.webGraph = webGraph;
        /* init node numbers so BVGraph gets +1 for each next node */
        NodeIterator it = this.nodeIterator();
        while (it.hasNext()) it.next();
    }

    public int numNodes () {
        return this.webGraph.numNodes();
    }

    public NodeIterator nodeIterator () {
        return this.webGraph.nodeIterator();
    }

    public MyImmutableSequentialGraph() {
        this(new SolrWebGraph("http://localhost:8898/solr/"));
    }
 
    public static ImmutableGraph load(CharSequence name, ProgressLogger unused) {
        return new MyImmutableSequentialGraph();
    }

    public static ImmutableGraph load(CharSequence name) {
        return MyImmutableSequentialGraph.load(name, (ProgressLogger) null);
    }

    public static ImmutableGraph loadSequential(CharSequence name, ProgressLogger pl) {
        return MyImmutableSequentialGraph.load(name, pl);
    }
        
    public static ImmutableGraph loadSequential(CharSequence name) {
        return MyImmutableSequentialGraph.loadSequential(name, (ProgressLogger) null);
    }

    public static ImmutableGraph loadOffline(CharSequence name, ProgressLogger pl) {
        return MyImmutableSequentialGraph.load(name, pl);
    }
    
    public static ImmutableGraph loadOffline(CharSequence name) {
        return MyImmutableSequentialGraph.loadOffline(name, (ProgressLogger) null);
    }
    
    public static ImmutableGraph loadOnce(InputStream is) {
        throw new RuntimeException("Cannot load my Webgraph from InputStream.");
    }
}