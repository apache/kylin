#!/bin/bash

/opt/entrypoint/hadoop/entrypoint.sh
/opt/entrypoint/hive/entrypoint.sh
/opt/entrypoint/hbase/entrypoint.sh

sleep 180

cd $KYLIN_HOME
sh bin/sample.sh
sh bin/kylin.sh start

while :
do
    sleep 100
done