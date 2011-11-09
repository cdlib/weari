package org.cdlib.was.ngIndexer;

import java.io.{File,InputStream};

import org.archive.io.{ArchiveReader}
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.arc.ARCReader;

/**
 * Wrapper for an ArchiveReader.
 */
class ArchiveReaderWrapper (wrapped : ArchiveReader) 
    extends Iterable[ArchiveRecordWrapper] {

  wrapped match {
    case arcReader : ARCReader => {
      arcReader.setParseHttpHeaders(false);
    }
  }
  
  def iterator : Iterator[ArchiveRecordWrapper] = {
    return new Iterator[ArchiveRecordWrapper] {
      private val it = wrapped.iterator;

      def hasNext = it.hasNext;
      
      def next = new ArchiveRecordWrapper(it.next, wrapped.getFileName);
    }
  }
      
  def close = wrapped.close;
}

object ArchiveReaderFactoryWrapper {
  def get (f : File) =
    new ArchiveReaderWrapper (ArchiveReaderFactory.get(f));

  def get (id : String, is : InputStream, atFirstRecord : Boolean) =
    new ArchiveReaderWrapper (ArchiveReaderFactory.get(id, is, atFirstRecord));
    
}
