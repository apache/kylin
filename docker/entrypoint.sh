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

# clean pid files
rm -f /tmp/*.pid

# start mysql
if [ ! -f "/home/admin/first_run" ]
then
    mysqld_pre_systemd
fi
mysqld --daemonize --pid-file=/var/run/mysqld/mysqld.pid -u root
if [ ! -f "/home/admin/first_run" ]
then
    mysql_init_password=`cat /var/log/mysqld.log |grep -Po '(?<=A temporary password is generated for root@localhost: )\S+'`
    mysql --connect-expired-password -u root -p$mysql_init_password -e "set global validate_password_policy=0;set global validate_password_length=6;alter user user() identified by '123456';"
    mysql -uroot -p123456 -e "grant all privileges on root.* to root@'%' identified by '123456';"
fi

# start hdfs
if [ ! -f "/home/admin/first_run" ]
then
    hdfs namenode -format
fi
$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode
$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode

# start yarn
$HADOOP_HOME/sbin/yarn-daemon.sh start resourcemanager
$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager

# start mr jobhistory
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

# start hbase
rm -rf /data/zookeeper/*
$HBASE_HOME/bin/start-hbase.sh

# start kafka
rm -rf /tmp/kafka-logs
nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &

# start livy
if [ ! -f "/home/admin/first_run" ]
then
    hdfs dfs -mkdir -p /kylin/livy
    hdfs dfs -put -f $HBASE_HOME/lib/hbase-client-$HBASE_VERSION.jar hdfs://localhost:9000/kylin/livy/
    hdfs dfs -put -f $HBASE_HOME/lib/hbase-common-$HBASE_VERSION.jar hdfs://localhost:9000/kylin/livy/
    hdfs dfs -put -f $HBASE_HOME/lib/hbase-hadoop-compat-$HBASE_VERSION.jar hdfs://localhost:9000/kylin/livy/
    hdfs dfs -put -f $HBASE_HOME/lib/hbase-hadoop2-compat-$HBASE_VERSION.jar hdfs://localhost:9000/kylin/livy/
    hdfs dfs -put -f $HBASE_HOME/lib/hbase-server-$HBASE_VERSION.jar hdfs://localhost:9000/kylin/livy/
    hdfs dfs -put -f $HBASE_HOME/lib/htrace-core-*-incubating.jar hdfs://localhost:9000/kylin/livy/
    hdfs dfs -put -f $HBASE_HOME/lib/metrics-core-*.jar hdfs://localhost:9000/kylin/livy/
    hdfs dfs -put -f $KYLIN_HOME/lib/kylin-job-$KYLIN_VERSION.jar hdfs://localhost:9000/kylin/livy/
fi
$LIVY_HOME/bin/livy-server start

mkdir -p ${KYLIN_HOME}/logs
# check hive usability first, this operation will insert one version record into VERSION table
$KYLIN_HOME/bin/check-hive-usability.sh > ${KYLIN_HOME}/logs/kylin-verbose.log 2>&1
# wait for starting hbase server successfully
sleep 20s

# prepare kafka topic and data
if [ ! -f "/home/admin/first_run" ]
then
    $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic kylin_streaming_topic
fi

# nohup $KYLIN_HOME/bin/kylin.sh org.apache.kylin.source.kafka.util.KafkaSampleProducer --topic kylin_streaming_topic --broker localhost:9092 > /dev/null 2>&1 > /tmp/kafka-sample.log &
# create sample cube at the first time
if [ ! -f "/home/admin/first_run" ]
then
    sh $KYLIN_HOME/bin/sample.sh >> ${KYLIN_HOME}/logs/kylin-verbose.log 2>&1
fi
touch /home/admin/first_run
# start kylin
$KYLIN_HOME/bin/kylin.sh -v start >> ${KYLIN_HOME}/logs/kylin-verbose.log 2>&1

while :
do
    sleep 1
done
