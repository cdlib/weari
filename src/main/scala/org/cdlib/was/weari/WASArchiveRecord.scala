/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import java.util.Date;

/**
 * Represents an ArchiveRecord (that is, one record in a (W)ARC file.
 *
 */
trait WASArchiveRecord {
  def getStatusCode : Int;

  def getFilename : String;

  def getUrl : String;
  
  def getDate : Date;

  def getDigestStr : Option[String];
    
  def getLength : Long;
  
  def isHttpResponse : Boolean;
  
  def getContentType : ContentType;
}
