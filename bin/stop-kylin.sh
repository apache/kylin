#!/bin/sh

pid=`cat ${KYLIN_HOME}/pid || echo "no pid found";exit 1`
if [ ! "$pid" = "" ]
then
    echo "no pid found"
    exit 1
else
    echo "stopping pid:$pid"
    kill $pid
fi
rm ${KYLIN_HOME}/pid
