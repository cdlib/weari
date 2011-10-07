/* Copyright (c) 2011 The Regents of the University of California */

package org.cdlib.was.ngIndexer;

import java.io._;
import java.util.zip._;
import scala.collection.mutable.ArrayBuffer;
import scala.io.Source;

class UrlCompressor (trainFile : String) {

  val deflater : Deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
  val inflater : Inflater = new Inflater(true);
  val EMPTYBYTEARRAY = new Array[Byte](0);

  // def train {
  //   val nullOS = new OutputStream() {
  //     override def write (b : Array[Byte]) = ()
  //     override def write (b : Int) = ()
  //     override def write (b : Array[Byte], off : Int, len : Int) = ()
  //   };
    
  //   val deflaterOS = new DeflaterOutputStream(nullOS, deflater);
  //   for (l <- Source.fromFile(trainFile).getLines.take(500)) {
  //     deflaterOS.write(l.getBytes);
  //   }
  //   if (!deflater.finished) {
  //     var buf = new Array[Byte](1024);
  //     def deflate {
  //       var n = 0;
  //       do {
  //         System.err.println(n);
  //       } while (deflater.deflate(buf) != 0)
  //     }
  //     // deflater.setInput(EMPTYBYTEARRAY, 0, 0);
  //     // deflater.setLevel(Deflater.NO_COMPRESSION);
  //     // System.err.println("NO_COMPRESSION");
  //     // deflate;
  //     // deflater.setLevel(Deflater.DEFAULT_COMPRESSION);
  //     // System.err.println("DEFAULT_COMPRESSION");
  //     // deflate;
  //   }
  //   deflater.reset;
  // }
  
//  train;

  def compress (url : String) : Array[Byte] = {
    var ab = new ArrayBuffer[Byte]();
    var n = 0;
    var buf = new Array[Byte](1024);

    deflater.setInput(url.getBytes("UTF-8"));
    def deflate {
      do {
        n = deflater.deflate(buf);
        System.err.println(n);
        if (n > 0) ab ++= buf.take(n);
      } while (n != 0);
    }
    deflate;
    if (deflater.needsInput) {
      deflater.setInput(EMPTYBYTEARRAY, 0, 0);
      deflater.setLevel(Deflater.NO_COMPRESSION);
      System.err.println("NO_COMPRESSION");
      deflate;
      deflater.setLevel(Deflater.DEFAULT_COMPRESSION);
      System.err.println("DEFAULT_COMPRESSION");
      deflate;
    }
    //deflater.reset;
    return ab.toArray;
  }

  def decompress (b : Array[Byte]) : String = {
    var ab = new ArrayBuffer[Byte]();
    var n = 0;
    var buf = new Array[Byte](1024);

    inflater.setInput(b);
    n = inflater.inflate(buf);
    while (n != 0) {
      ab ++= buf.take(n);
      n = inflater.inflate(buf);
    }
    inflater.reset;
    return Source.fromBytes(ab.toArray).mkString;
  }

  def test {
    var s = 0.0;
    var sc = 0.0;
    var n = 0;
    for (l <- Source.fromFile(trainFile).getLines.drop(500)) {     
      try {
        n = n + 1;
        val cl = compress(l);
        s += l.size;
        sc += cl.size;
        assert (l == decompress(cl));
      } catch {
        case ex : EOFException => {
          ex.printStackTrace(System.err);
        }
      }
    }
    System.out.println("compressed %d small strings with a ratio of %f".format(n, sc/s));

  }
}
