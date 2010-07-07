#!/bin/sh

BASEDIR=`dirname $0`

CLASSPATH=$BASEDIR/classes:$HOME/d/software/scala-2.7.7.final/lib/scala-library.jar
for x in `ls -1 $BASEDIR/lib` ; do
    CLASSPATH=$CLASSPATH:$BASEDIR/lib/$x
done

java -Xmx2048m -cp $CLASSPATH -Dorg.cdlib.was.ngIndexer.ConfigFile=$BASEDIR/config.js org.cdlib.was.ngIndexer.solrIndexer $*
