/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari.webgraph;

import java.util.Date;
import org.archive.net.UURI;

class Outlink (val from : UURI,
               val to : UURI,
               val date : Date,
               val text : String) {}
