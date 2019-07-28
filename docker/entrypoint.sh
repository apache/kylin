#!/bin/bash

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

# prepare kafka topic and data
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic kylin_streaming_topic

while :
do
    sleep 1
done
