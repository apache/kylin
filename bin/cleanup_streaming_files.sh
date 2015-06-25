#!/usr/bin/env bash

if [ $# != 1 ]
then
    echo 'invalid input' || exit -1
fi

cd $KYLIN_HOME

for pidfile in `find . -name "$1_1*"`
do
    pidfile=`echo "$pidfile" | cut -c 3-`
    echo "pidfile:$pidfile"
    pid=`cat $pidfile`
    if [ `ps -ef | awk '{print $2}' | grep -w $pid | wc -l` = 1 ]
    then
        echo "pid:$pid still running"
    else
        echo "pid:$pid not running, try to delete files"
        echo $pidfile | xargs rm
        echo "logs/streaming_$pidfile.log" | xargs rm
    fi
done

