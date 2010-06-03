package org.cdlib.was.ngIndexer;

import java.util.Date;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.cdlib.rabinpoly.RabinPoly;

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

  val FINGERPRINT_PT = 0xbfe6b8a5bf378d83L;

  var rb = new RabinPoly(FINGERPRINT_PT);

  def fingerprint (url : UURI) : Long = {
    var fp = 0L;
    for (byte <- url.getEscapedURI.getBytes("UTF-8")) {
      fp = rb.append8(fp, byte);
    }
    return fp;
  }

  def long2bytearray (l : Long) : Array[Byte] = {
    var fbBytes = Array.make[Byte](8, 0);
    for (i <- new Range(0, 8, 1)) {
      fbBytes(7-i) = (l >>> i*8).asInstanceOf[Byte];
    }
    return fbBytes;
  }

  def bytearray2long (bytes : Array[Byte]) : Long = {
    var retval = 0L;
    for (i <- new Range(0, 8, 1)) {
      retval = retval | ((bytes(i) & 0xFF).asInstanceOf[Long] << (7-i)*8);
    }
    return retval;
  }

  def int2bytearray (i : Int) : Array[Byte] = {
    var fbBytes = Array.make[Byte](4, 0);
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
