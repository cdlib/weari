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

import grizzled.slf4j.Logging;

import java.util.Date;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.URIException;

import org.archive.url.{DefaultIAURLCanonicalizer,HandyURL,URLParser};

import org.cdlib.rabinpoly.RabinPoly;

object UriUtils extends Logging {
  val canonicalizer = new DefaultIAURLCanonicalizer();
  
  def string2handyUrl (s : String) : HandyURL = 
    URLParser.parse(s);

  def canonicalize (s : String) : String = {
    return try {
      val handyurl = string2handyUrl(s);
      canonicalizer.canonicalize(handyurl);
      handyurl.getURLString;
    } catch {
      case ex : URIException => s;
      case ex : java.lang.IndexOutOfBoundsException => s;
    }
  }

  def encodeDate (date : Date) : Array[Byte] =
    UriUtils.int2bytearray((date.getTime/1000).asInstanceOf[Int]);

  def decodeDate (bytes : Array[Byte]) : Date = 
    new Date(UriUtils.bytearray2int(bytes)*1000);
  
  def encodeFp(fp : Long) = long2bytearray(fp);

  def decodeFp(bytes : Array[Byte]) = bytearray2long(bytes);

  def encodeUrlFpDate(url : String, date : Date) : Array[Byte] = 
    encodeFpDate(fingerprint(url), date);

  def encodeFpDate(fp : Long, date : Date) : Array[Byte] = 
    encodeFp(fp) ++ encodeDate(date);

  def decodeUrlFPDate(bytes : Array[Byte]) : Pair[Long, Date] = 
    Pair(decodeFp(bytes.take(8)), decodeDate(bytes.take(4)));
  
  def fingerprint (url : String) : Long = 
    RabinPoly.fingerprint(url);

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
