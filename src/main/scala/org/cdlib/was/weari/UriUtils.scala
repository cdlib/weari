/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.weari;

import java.util.Date;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.cdlib.rabinpoly.RabinPoly;
import org.apache.commons.codec.binary.Base64;

object UriUtils {
    
  def encodeDate (date : Date) : Array[Byte] =
    UriUtils.int2bytearray((date.getTime/1000).asInstanceOf[Int]);

  def decodeDate (bytes : Array[Byte]) : Date = 
    new Date(UriUtils.bytearray2int(bytes)*1000);
  
  /* simple for now */
  def encodeUrl (url : UURI) : Array[Byte] = 
    url.getEscapedURI.getBytes("UTF-8");
  
  def decodeUrl (bytes : Array[Byte]) = 
    UURIFactory.getInstance(new String(bytes));

  def encodeFp(fp : Long) = long2bytearray(fp);

  def decodeFp(bytes : Array[Byte]) = bytearray2long(bytes);

  def encodeUrlFpDate(url : UURI, date : Date) : Array[Byte] = 
    encodeFpDate(fingerprint(url), date);

  def encodeFpDate(fp : Long, date : Date) : Array[Byte] = 
    encodeFp(fp) ++ encodeDate(date);

  def decodeUrlFPDate(bytes : Array[Byte]) : Pair[Long, Date] = 
    Pair(decodeFp(bytes.take(8)), decodeDate(bytes.take(4)));
  
  def fingerprint (url : UURI) : Long = 
    RabinPoly.fingerprint(url.getEscapedURI);

  def fp2string (fp : Long) : String =
    new String(Base64.encodeBase64(this.encodeFp(fp)));

  def string2fp (s : String) : Long =
    this.decodeFp(Base64.decodeBase64(s.getBytes("UTF-8")));

  def fpdate2string (fp : Long, date : Date) : String =
    new String(Base64.encodeBase64(this.encodeFpDate(fp, date)));

  def long2bytearray (l : Long) : Array[Byte] = {
    var fbBytes = Array.fill[Byte](8)(0);
    for (i <- new Range(0, 8, 1)) {
      fbBytes(i) = (l >>> 8*(7-i)).asInstanceOf[Byte];
    }
    return fbBytes;
  }

  def long2string (l : Long) : String = {
    var fbBytes = Array.fill[Byte](8)(0);
    for (i <- new Range(0, 8, 1)) {
      fbBytes(i) = (l >>> 8*(7-i)).asInstanceOf[Byte];
    }
    return "%016x".format(l);
  }

  def bytearray2long (bytes : Array[Byte]) : Long = {
    var retval = 0L;
    for (i <- new Range(0, 8, 1)) {
      retval = retval | (((bytes(i) & 0xFFL) << (7-i)*8));
    }
    return retval;
  }

  def int2bytearray (i : Int) : Array[Byte] = {
    var fbBytes = Array.fill[Byte](4)(0);
    for (j <- new Range(0, 4, 1)) {
      fbBytes(3-j) = (i >>> j*8).asInstanceOf[Byte];
    }
    return fbBytes;
  }
  
  def bytearray2int (bytes : Array[Byte]) : Int = {
    var retval = 0;
    for (i <- new Range(0, 4, 1)) {
      retval = retval | ((bytes(i) & 0xFF).asInstanceOf[Int] << (3-i)*8);
    }
    return retval;
  }
}
