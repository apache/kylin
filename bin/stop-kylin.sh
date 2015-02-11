#!/bin/sh

if [ ! -f "${KYLIN_HOME}/pid" ]
then
    echo "no pid found"
    exit 1
fi

pid=`cat ${KYLIN_HOME}/pid`
if [ "$pid" = "" ]
then
    echo "pid is empty"
    exit 1
else
    echo "stopping kylin:$pid"
    kill $pid
fi
rm ${KYLIN_HOME}/pid
