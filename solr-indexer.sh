#!/bin/sh

BASEDIR=`dirname $0`

CLASSPATH=$BASEDIR/classes:$HOME/d/software/scala/lib/scala-library.jar:$HOME/d/software/scala/lib/scala-compiler.jar
for x in `ls -1 $BASEDIR/lib` ; do
    CLASSPATH=$CLASSPATH:$BASEDIR/lib/$x
done

java -Xmx2048m -cp $CLASSPATH -Dorg.cdlib.was.ngIndexer.ConfigFile=$BASEDIR/indexer.conf org.cdlib.was.ngIndexer.SolrIndexer $*
