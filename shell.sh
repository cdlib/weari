#!/bin/sh

BASEDIR=`dirname $0`
CLASSPATH=$BASEDIR/classes
for x in `ls -1 $BASEDIR/lib` ; do
    CLASSPATH=$CLASSPATH:$BASEDIR/lib/$x
done

JAVA_OPTS="-Xmx2048M -Xms32M" ; export JAVA_OPTS

~/d/software/scala/bin/scala -cp $CLASSPATH
