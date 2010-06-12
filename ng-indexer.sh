#!/bin/sh

BASEDIR=`dirname $0`

CLASSPATH=$BASEDIR/classes:$HOME/d/software/scala-2.7.7.final/lib/scala-library.jar
for x in `ls -1 $BASEDIR/lib` ; do
    CLASSPATH=$CLASSPATH:$BASEDIR/lib/$x
done

java -Xmx2048m -cp $CLASSPATH org.cdlib.was.ngIndexer.ngIndexer $*
