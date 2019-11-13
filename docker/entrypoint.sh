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
echo "127.0.0.1 sandbox.hortonworks.com" >> /etc/hosts

# start mysql
service mysqld start
mysqladmin -uroot password 123456
mysql -uroot -p123456 -e "grant all privileges on root.* to root@'%' identified by '123456';"

# start hdfs
if [ ! -f "/home/admin/first_run" ]
then
    hdfs namenode -format
fi
touch /home/admin/first_run
$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode
$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode

# start yarn
$HADOOP_HOME/sbin/yarn-daemon.sh start resourcemanager
$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager

# start mr jobhistory
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

# start hbase
$HBASE_HOME/bin/start-hbase.sh

# start kafka
rm -rf /tmp/kafka-logs
rm -rf /data/zookeeper/*
nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &

# start livy
hdfs dfs -mkdir -p /kylin/livy
hdfs dfs -put -f $HBASE_HOME/lib/hbase-client-$HBASE_VERSION.jar hdfs://localhost:9000/kylin/livy/
hdfs dfs -put -f $HBASE_HOME/lib/hbase-common-$HBASE_VERSION.jar hdfs://localhost:9000/kylin/livy/
hdfs dfs -put -f $HBASE_HOME/lib/hbase-hadoop-compat-$HBASE_VERSION.jar hdfs://localhost:9000/kylin/livy/
hdfs dfs -put -f $HBASE_HOME/lib/hbase-hadoop2-compat-$HBASE_VERSION.jar hdfs://localhost:9000/kylin/livy/
hdfs dfs -put -f $HBASE_HOME/lib/hbase-server-$HBASE_VERSION.jar hdfs://localhost:9000/kylin/livy/
hdfs dfs -put -f $HBASE_HOME/lib/htrace-core-*-incubating.jar hdfs://localhost:9000/kylin/livy/
hdfs dfs -put -f $HBASE_HOME/lib/metrics-core-*.jar hdfs://localhost:9000/kylin/livy/
hdfs dfs -put -f $KYLIN_HOME/lib/kylin-job-$KYLIN_VERSION.jar hdfs://localhost:9000/kylin/livy/
$LIVY_HOME/bin/livy-server start

# prepare kafka topic and data
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic kylin_streaming_topic

while :
do
    sleep 1
done
