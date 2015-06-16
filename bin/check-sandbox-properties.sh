#!/usr/bin/env bash

result=
for i in `cat ${KYLIN_HOME}/conf/kylin.properties | grep 'kylin.sandbox' | grep -v '^#' |awk -F '=' '{print $2}' | cut -c 1-4`
do
   :
   result=$i
done
echo $result