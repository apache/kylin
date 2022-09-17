#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
echo "127.0.0.1 sandbox" >> /etc/hosts

# clean pid files
rm -f /tmp/*.pid

# start mysql
service mysql start
if [ ! -f "/home/kylin/first_run" ]
then
    mysql -uroot -p123456 -e "CREATE DATABASE IF NOT EXISTS kylin default charset utf8mb4 COLLATE utf8mb4_general_ci;"
fi

# init schema for hive
if [ ! -f "/home/kylin/first_run" ]
then
    schematool -initSchema -dbType mysql
fi

# start hdfs
if [ ! -f "/home/kylin/first_run" ]
then
    hdfs namenode -format
fi

hdfs --daemon start namenode
hdfs --daemon start datanode

# start yarn
yarn --daemon start resourcemanager
yarn --daemon start nodemanager

# start job history server
mapred --daemon start historyserver

# start zk
rm -rf /data/zookeeper/*
rm -f /data/zookeeper/zookeeper_server.pid
$ZK_HOME/bin/zkServer.sh start

sleep 10s
mkdir -p $KYLIN_HOME/logs

function check_hdfs_usability() {
    echo "Checking HDFS's service..."
    started_hdfs=
    ((time_left = 60))
    while ((time_left > 0)); do
        hdfs dfs -test -d /tmp
        started_hdfs=$?
        if [[ $started_hdfs -eq 0 ]]; then
            break
        fi
        sleep 5
        ((timeLeft -= 5))
    done
    if [[ $started_hdfs -eq 0 ]]; then
        echo "HDFS's service started..."
    else
        echo "ERROR: Check HDFS's service failed, please check the status of your cluster"
    fi
}

if [ ! -f "/home/kylin/first_run" ]
then
    # check hdfs usability first if hdfs service was not started normally
    check_hdfs_usability
    hdfs dfs -mkdir -p /kylin
fi

# create sample data at the first time
if [ ! -f "/home/kylin/first_run" ]
then
    $KYLIN_HOME/bin/sample.sh >> ${KYLIN_HOME}/logs/kylin-verbose.log 2>&1
fi

# start kylin
$KYLIN_HOME/bin/kylin.sh -v start >> ${KYLIN_HOME}/logs/kylin-verbose.log 2>&1



touch /home/kylin/first_run

while :
do
    sleep 10
done
