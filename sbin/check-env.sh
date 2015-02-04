#!/bin/sh

if [ -z "$KYLIN_HOME" ]
then
    echo 'please make sure KYLIN_HOME has been set'
    exit 1
fi

if [ -z "$CATALINA_HOME" ]
then
    echo "Please set CATALINA_HOME so that Kylin knows where to start tomcat"
    exit 1
else
    echo "CATALINA_HOME is set to $CATALINA_HOME"
fi

if [ ! -z "$KYLIN_LD_LIBRARY_PATH" ]
then
    echo "KYLIN_LD_LIBRARY_PATH is set to $KYLIN_LD_LIBRARY_PATH"
else
    exit 1
fi