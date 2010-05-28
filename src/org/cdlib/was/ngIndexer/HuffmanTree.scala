package org.cdlib.was.ngIndexer;

import scala.collection.mutable.ListBuffer;
import scala.io.Source;
import java.io.EOFException;
import scala.collection.mutable.HashMap;

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
  val trainFile = "/home/egh/w/ng-indexer/url_deflate_train";
  val freqTable = BuildHuffmanTree.buildFreqTable(trainFile);
  val tree = BuildHuffmanTree.buildDecoderTree(freqTable._1, freqTable._2);
  val encoderList = BuildHuffmanTree.buildEncoderList(tree);
  val encoderMap = BuildHuffmanTree.buildEncoderMap(tree);
  
  val zeroByteList = List[Byte](0);
  val emptyBooleanList = List[Boolean]();
  val nullTerm = LeafNode(zeroByteList);

  def encodeList (bytes : List[Byte]) : List[Boolean] = {
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
        acc = acc ++ encoderMap.get(toEncode.first).getOrElse(emptyBooleanList);
        toEncode = toEncode.tail;
      }
    }
    return acc;
  }

  def encode (s : Seq[Byte]) : Seq[Boolean] = this.encodeList(s.toList);
  
  def encode (s : String) : Seq[Boolean] = this.encode(s.getBytes("UTF-8"));
  
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
  
  def decode (s : Seq[Boolean]) : Seq[Byte] =
    decodeList (s.toList);

  def decodeToString (s : Seq[Boolean]) : String =
    Source.fromBytes(decodeList (s.toList).toArray).mkString;
  
  def decodeList (s : List[Boolean]) : List[Byte] = s match {
    case Nil => return Nil;
    case _   => return decodeList(s.toList, tree);
  }
  
  def decodeList (s : List[Boolean], sofar : DecoderNode) : List[Byte] = {
    if (sofar == nullTerm) {
      Nil;
    } else {
      sofar match {
        case l : LeafNode     => l.bytes.toList ++ decodeList(s);
        case i : InternalNode => s match {
          case false :: xs => decodeList(xs, i.left);
          case true  :: xs => decodeList(xs, i.right);
          case Nil         => throw new RuntimeException("Bad sequence!");
        }
      }
    }
  }
    
  def test {
    var s = 0.0;
    var sc = 0.0;
    var n = 0;
    for (l <- Source.fromFile(trainFile).getLines) {     
      try {
        n = n + 1;
        val chomped = l.take(l.size-1);
        val encoded = encode(chomped);
        s += chomped.size;
        sc += encoded.size / 8;
        val decoded = decodeToString(encoded);
        if (chomped.toList != decoded.toList) {
          System.out.println("'%s' != '%s'".format(chomped,
                                                   decoded));
        }
      } catch {
        case ex : EOFException => {
          ex.printStackTrace(System.err);
        }
      }
    }
    System.out.println("compressed %d small strings with a ratio of %f".format(n, sc/s));

  }
}
