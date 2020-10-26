#!/bin/bash

/opt/entrypoint/hadoop/entrypoint.sh
/opt/entrypoint/hive/entrypoint.sh
/opt/entrypoint/hbase/entrypoint.sh

sleep 100

cd $KYLIN_HOME
sh bin/kylin.sh start

# Only one node execute sample.sh
if [[ $HOSTNAME =~ "kylin-all" ]]
then
  sh bin/sample.sh start
fi

while :
do
    sleep 100
done