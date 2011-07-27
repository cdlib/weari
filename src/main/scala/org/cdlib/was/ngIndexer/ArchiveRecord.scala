package org.cdlib.was.ngIndexer;

import java.util.Date;

trait IndexArchiveRecord extends ContentType {
  def getStatusCode : Int;

  def getFilename : String;

  def getUrl : String;
  
  def getDate : Date;
  
  def getDigestStr : Option[String];
    
  def getLength : Long;
  
  def isHttpResponse : Boolean;
}
