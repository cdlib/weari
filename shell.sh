#!/bin/sh

BASEDIR=`dirname $0`
CLASSPATH=$BASEDIR/classes
for x in `ls -1 $BASEDIR/lib` ; do
    CLASSPATH=$CLASSPATH:$BASEDIR/lib/$x
done

JAVA_OPTS="-Xmx1024M -Xms32M" ; export JAVA_OPTS

~/d/software/scala-2.7.7.final/bin/scala -cp $CLASSPATH
