#!/bin/bash

if [ $# != 1 ]
then
    echo 'invalid input'
    exit -1
fi

result=
for i in `cat ${KYLIN_HOME}/conf/kylin.properties | grep -w "$1" | grep -v '^#' |awk -F '=' '{print $2}' | cut -c 1-`
do
   :
   result=$i
done
echo $result