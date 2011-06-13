package org.cdlib.was.ngIndexer;

import java.util.Date;

trait IndexArchiveRecord {
  def getStatusCode : Int;

  def getMediaType : Option[Pair[String,String]];

  lazy val getMediaTypeStr = 
    "%s/%s".format(getMediaTopType, getMediaSubType);

  lazy val getMediaTopType : String = {
    getMediaType.map(_._1).getOrElse("application")
  }

  lazy val getMediaSubType : String = {
    getMediaType.map(_._2).getOrElse("octet-stream")
  }

  def getCharset : Option[String];

  def getFilename : String;

  def getUrl : String;
  
  def getDate : Date;
  
  def getDigestStr : Option[String];
    
  def getLength : Long;
  
  def isHttpResponse : Boolean;
}
