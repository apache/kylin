#!/bin/bash

/opt/entrypoint/hadoop/entrypoint.sh
/opt/entrypoint/hive/entrypoint.sh
/opt/entrypoint/hbase/entrypoint.sh

while :
do
    sleep 1000
done