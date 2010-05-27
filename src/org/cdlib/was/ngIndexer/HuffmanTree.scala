package org.cdlib.was.ngIndexer;

import scala.collection.mutable.ListBuffer;
import scala.io.Source;
import java.io.EOFException;

abstract class HuffmanNode;

sealed case class LeafNode (val byte : Byte) extends HuffmanNode {
  override def toString = "(->%c)".format(byte);
}

sealed case class InternalNode (val left : HuffmanNode, 
                         val right : HuffmanNode)
  extends HuffmanNode {
    override def toString = "(0->%s, 1->%s)".format(left, right);
}

/* Calculated Huffman tree for encoding URIs */

object HuffmanEncoder {
  val trainFile = "/home/egh/w/ng-indexer/url_deflate_train";
  val freqTable = BuildHuffmanTree.buildFreqTable(trainFile);
  val tree = BuildHuffmanTree.buildTree(freqTable._1, freqTable._2);
  val printTable = BuildHuffmanTree.buildPrintTable(tree);
  
  val zeroByteList = List[Byte](0);
  val emptyBooleanList = List[Boolean]();

  def encode (s : Seq[Byte]) : Seq[Boolean] = {
    var b = new ListBuffer[Boolean]();
    val nullTermS = s ++ zeroByteList;
    return nullTermS.flatMap(printTable.get(_).getOrElse(emptyBooleanList));
  }
  
  def encode (s : String) : Seq[Boolean] =
    this.encode(s.getBytes("UTF-8"));
  
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
  
//  def byteSeq2boolSeq (s : Seq[Byte])
  
  def decode (s : Seq[Boolean]) : Seq[Byte] =
    decodeList (s.toList);

  def decodeToString (s : Seq[Boolean]) : String =
    Source.fromBytes(decodeList (s.toList).toArray).mkString;
  
  def decodeList (s : List[Boolean]) : List[Byte] = s match {
    case Nil => return Nil;
    case _   => return decodeList(s.toList, tree);
  }
  
  def decodeList (s : List[Boolean], sofar : HuffmanNode) : List[Byte] = {
    sofar match {
      case LeafNode(0)      => Nil; /* NULL terminated */
      case l : LeafNode     => l.byte :: decodeList(s);
      case i : InternalNode => s match {
        case false :: xs => decodeList(xs, i.left);
        case true  :: xs => decodeList(xs, i.right);
        case Nil         => throw new RuntimeException("Bad sequence!");
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
