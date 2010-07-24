package org.cdlib.was.ngIndexer;

import scala.collection.mutable.ListBuffer;
import scala.io.Source;
import java.io.EOFException;
import scala.collection.mutable.HashMap;
import org.archive.net.UURIFactory;

abstract class DecoderNode;

sealed case class LeafNode (val bytes : Seq[Byte]) extends DecoderNode {
  override def toString = "(->%c)".format(bytes);
}

sealed case class InternalNode (val left : DecoderNode, 
                                val right : DecoderNode)
  extends DecoderNode {
    override def toString = "(0->%s, 1->%s)".format(left, right);
}

/* Calculated Huffman tree for encoding URIs */

object HuffmanEncoder {
  val tree = new InternalNode(new InternalNode(new InternalNode(new InternalNode(new InternalNode(new LeafNode(List[Byte](115)), new LeafNode(List[Byte](110))), new InternalNode(new InternalNode(new InternalNode(new LeafNode(List[Byte](121)), new LeafNode(List[Byte](102))), new LeafNode(List[Byte](100))), new LeafNode(List[Byte](114)))), new InternalNode(new InternalNode(new LeafNode(List[Byte](105)), new InternalNode(new LeafNode(List[Byte](117)), new InternalNode(new LeafNode(List[Byte](46,111,114,103,47)), new LeafNode(List[Byte](107))))), new InternalNode(new LeafNode(List[Byte](109)), new LeafNode(List[Byte](97))))), new LeafNode(List[Byte](104,116,116,112,58,47,47))), new InternalNode(new InternalNode(new InternalNode(new InternalNode(new LeafNode(List[Byte](0)), new LeafNode(List[Byte](58))), new InternalNode(new LeafNode(List[Byte](99)), new InternalNode(new LeafNode(List[Byte](108)), new InternalNode(new InternalNode(new InternalNode(new InternalNode(new InternalNode(new LeafNode(List[Byte](53)), new LeafNode(List[Byte](56))), new LeafNode(List[Byte](50))), new LeafNode(List[Byte](120))), new LeafNode(List[Byte](45))), new LeafNode(List[Byte](98)))))), new InternalNode(new InternalNode(new LeafNode(List[Byte](46)), new LeafNode(List[Byte](101))), new InternalNode(new LeafNode(List[Byte](104)), new InternalNode(new InternalNode(new InternalNode(new LeafNode(List[Byte](118)), new LeafNode(List[Byte](119))), new LeafNode(List[Byte](46,110,101,116,47))), new InternalNode(new InternalNode(new InternalNode(new LeafNode(List[Byte](106)), new InternalNode(new LeafNode(List[Byte](49)), new InternalNode(new LeafNode(List[Byte](52)), new LeafNode(List[Byte](113))))), new InternalNode(new LeafNode(List[Byte](122)), new InternalNode(new InternalNode(new LeafNode(List[Byte](51)), new InternalNode(new InternalNode(new InternalNode(new InternalNode(new InternalNode(new LeafNode(List[Byte](84)), new InternalNode(new LeafNode(List[Byte](72)), new LeafNode(List[Byte](73)))), new InternalNode(new InternalNode(new LeafNode(List[Byte](71)), new LeafNode(List[Byte](76))), new InternalNode(new InternalNode(new LeafNode(List[Byte](74)), new LeafNode(List[Byte](85))), new LeafNode(List[Byte](80))))), new InternalNode(new InternalNode(new LeafNode(List[Byte](83)), new InternalNode(new LeafNode(List[Byte](82)), new LeafNode(List[Byte](70)))), new InternalNode(new InternalNode(new LeafNode(List[Byte](78)), new InternalNode(new InternalNode(new InternalNode(new InternalNode(new InternalNode(new InternalNode(new InternalNode(new LeafNode(List[Byte](91)), new LeafNode(List[Byte](39))), new InternalNode(new LeafNode(List[Byte](36)), new LeafNode(List[Byte](59)))), new InternalNode(new InternalNode(new LeafNode(List[Byte](40)), new LeafNode(List[Byte](63))), new InternalNode(new LeafNode(List[Byte](38)), new InternalNode(new LeafNode(List[Byte](64)), new LeafNode(List[Byte](35)))))), new InternalNode(new LeafNode(List[Byte](61)), new InternalNode(new InternalNode(new LeafNode(List[Byte](43)), new LeafNode(List[Byte](44))), new InternalNode(new InternalNode(new LeafNode(List[Byte](93)), new LeafNode(List[Byte](33))), new InternalNode(new LeafNode(List[Byte](42)), new LeafNode(List[Byte](124))))))), new LeafNode(List[Byte](81))), new LeafNode(List[Byte](89))), new LeafNode(List[Byte](87)))), new InternalNode(new LeafNode(List[Byte](69)), new LeafNode(List[Byte](66)))))), new InternalNode(new InternalNode(new InternalNode(new LeafNode(List[Byte](95)), new InternalNode(new LeafNode(List[Byte](68)), new InternalNode(new LeafNode(List[Byte](86)), new InternalNode(new InternalNode(new LeafNode(List[Byte](88)), new LeafNode(List[Byte](90))), new LeafNode(List[Byte](37)))))), new InternalNode(new InternalNode(new LeafNode(List[Byte](67)), new InternalNode(new LeafNode(List[Byte](75)), new LeafNode(List[Byte](79)))), new InternalNode(new LeafNode(List[Byte](77)), new LeafNode(List[Byte](65))))), new LeafNode(List[Byte](126)))), new LeafNode(List[Byte](55)))), new InternalNode(new InternalNode(new LeafNode(List[Byte](57)), new LeafNode(List[Byte](54))), new LeafNode(List[Byte](48)))))), new LeafNode(List[Byte](103))))))), new InternalNode(new InternalNode(new LeafNode(List[Byte](116)), new LeafNode(List[Byte](46,99,111,109,47))), new InternalNode(new InternalNode(new LeafNode(List[Byte](112)), new LeafNode(List[Byte](111))), new LeafNode(List[Byte](47))))));
  val encoderList = BuildHuffmanTree.buildEncoderList(tree);
  val encoderMap = BuildHuffmanTree.buildEncoderMap(tree);
  
  val zeroByteList = List[Byte](0);
  val emptyBooleanList = List[Boolean]();
  val nullTerm = LeafNode(zeroByteList);

  def encodeByteList (bytes : List[Byte]) : List[Boolean] = {
    var toEncode = bytes;
    var acc = emptyBooleanList;
    while (toEncode.size > 0) {
      for (p <- encoderList) {
        val toMatch = p._1;
        if (toMatch == toEncode.take(toMatch.size)) {
          toEncode = toEncode.drop(toMatch.size);
          acc = acc ++ p._2;
        }
      }
      if (toEncode.size > 0) {
        acc = acc ++ encoderMap.get(toEncode.head).getOrElse(emptyBooleanList);
        toEncode = toEncode.tail;
      }
    }
    return acc;
  }

  def encode2BoolSeq (s : Seq[Byte]) : Seq[Boolean] = this.encodeByteList(s.toList);

  def encode (bytes : Seq[Byte]) : Seq[Byte] =
    this.boolSeq2byteSeq(this.encodeByteList(bytes.toList ::: zeroByteList));

  def encode (s : String) : Seq[Byte] = this.encode(s.getBytes("UTF-8"))
  
  def boolSeq2byteSeq (s : Seq[Boolean]) : Seq[Byte] = {
    var size = s.size/8;
    if ((s.size % 8) > 0) size = size + 1;
    var a = new Array[Byte](size);
    var i = 0;
    for (bool <- s) {
      if (bool) {
        a(i / 8) = (a(i / 8) | (1 << (i % 8))).asInstanceOf[Byte];
      }
      i = i + 1;
    }
    return a;
  }

  def byteSeq2BoolSeq (s : Seq[Byte]) : Seq[Boolean] = {
    var size = s.size*8;
    var a = new Array[Boolean](size);
    var i = 0;
    for (b <- s;
         j <- new Range(0, 8, 1)) {
           a(i*8 + j) = ((b & (1 << j)) != 0);
           if (j == 7) { i = i + 1; }
         }
    return a;
  }

  def decode (s : Seq[Byte]) : String =
    new String(decode2ByteSeq(s).toArray);

  def decode2ByteSeq (s : Seq[Byte]) : Seq[Byte] =
    decodeBoolSeq (byteSeq2BoolSeq(s));
  
  def decodeBoolSeq (s : Seq[Boolean]) : Seq[Byte] =
    decodeBoolList (s.toList);
  
  def decodeBoolList (s : List[Boolean]) : List[Byte] = s match {
    case Nil => return Nil;
    case _   => return decodeBoolList(s.toList, tree);
  }
  
  def decodeBoolList (s : List[Boolean], sofar : DecoderNode) : List[Byte] = {
    if (sofar == nullTerm) {
      Nil;
    } else {
      sofar match {
        case l : LeafNode     => l.bytes.toList ++ decodeBoolList(s);
        case i : InternalNode => s match {
          case false :: xs => decodeBoolList(xs, i.left);
          case true  :: xs => decodeBoolList(xs, i.right);
          case Nil         => throw new RuntimeException("Bad sequence!");
        }
      }
    }
  }
    
  def test (testFile : String) {
    var s = 0.0;
    var sc = 0.0;
    var n = 0;
    for (l <- Source.fromFile(testFile).getLines) {     
      try {
        n = n + 1;
        val chomped = UURIFactory.getInstance(l.take(l.size-1)).getEscapedURI;
        val encoded = encode(chomped);
        s += chomped.size;
        sc += encoded.size;
        val decoded = decode(encoded);
        if (chomped.toList != decoded.toList) {
          System.out.println("'%s' != '%s'".format(chomped,
                                                   decoded));
        }
      } catch {
        case ex : EOFException => ex.printStackTrace(System.err);
        case ex : org.apache.commons.httpclient.URIException =>
          ex.printStackTrace(System.err);
      }
    }
    System.out.println("Compressed %d URLs with a ratio of %f".format(n, sc/s));
  }
}
