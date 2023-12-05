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
sed '/127.0.0.1/d' /etc/hosts
echo "${OUTER_HOST:-127.0.0.1} Kylin5-Machine" >> /etc/hosts
cat /etc/hosts
START_FLAG="/home/kylin/first_run"
TIMEOUT=600

function run_command {
  local STEP="$1"
  shift 1

  echo ""
  echo "==============================================================================="
  echo "*******************************************************************************"
  echo "|"
  echo "|   $STEP at $(date)"
  echo "|   Command: $@"
  echo "|"
  "$@" 2>&1

  local EC=$?
  if [ $EC != 0 ]; then
    echo "ERROR!!"
    echo "[$STEP] Command FAILED : $@, please check!!!"
    sleep 7200
    exit $EC
  else
    echo "| $STEP succeed."
  fi
}

function check_and_monitor_status() {
  local COMPONENT="$1"
  shift 1
  echo "Checking $COMPONENT's status..."
  component_status=
  ((time_left = TIMEOUT))
  while ((time_left > 0)); do
      sleep 10
      "$@" 2>&1
      component_status=$?
      if [[ $component_status -eq 0 ]]; then
          echo "+"
          break
      else
          echo "-"
      fi
      ((timeLeft -= 10))
  done
  if [[ $component_status -eq 0 ]]; then
      echo "Check $COMPONENT succeed."
  else
      echo "ERROR: check $COMPONENT failed."
  fi
  return $component_status
}

# clean pid files
rm -f /tmp/*.pid
rm -rf /data/zookeeper/*
rm -f /data/zookeeper/zookeeper_server.pid

##############################################
##############################################
##############################################

run_command "Start MySQL server" service mysql start
if [ ! -f $START_FLAG ]
then
    sed -i 's/127.0.0.1/0.0.0.0/g' /etc/mysql/mysql.conf.d/mysqld.cnf
    sed -i 's/localhost/kylin5-machine/g' /opt/hadoop-3.2.1/etc/hadoop/workers
    mysql -uroot -p123456 mysql -e "update user set host='%' where user='root';  grant all privileges on *.* to root@'%';FLUSH PRIVILEGES;"
    run_command "Create Database" mysql -uroot -p123456 -e "CREATE DATABASE IF NOT EXISTS kylin default charset utf8mb4 COLLATE utf8mb4_general_ci;"
    run_command "Init Hive Metastore" schematool -initSchema -dbType mysql
    run_command "Format HDFS" hdfs namenode -format
fi

run_command "Start HDFS [NameNode]" hdfs --daemon stop namenode; hdfs --daemon start namenode
run_command "Start HDFS [DataNode]" hdfs --daemon stop datanode; hdfs --daemon start datanode

run_command "Start Yarn [ResourceManager]" yarn --daemon stop resourcemanager; yarn --daemon start resourcemanager
run_command "Start Yarn [NodeManager]" yarn --daemon stop nodemanager; yarn --daemon start nodemanager

run_command "Start Zookeeper" "$ZK_HOME"/bin/zkServer.sh start

sleep 10s

if [ ! -f $START_FLAG ]
then
    check_and_monitor_status "HDFS Usability" hadoop fs -mkdir /tmp
    mkdir -p "$KYLIN_HOME"/logs
    hdfs dfs -mkdir -p /kylin
    if [ -f "/home/kylin/tpch-sf2.zip" ]
    then
        cd /home/kylin/scripts || exit
        unzip tpch-sf2.zip
        run_command "Update tpch tables comments" service mysql restart; mysql -u root -p123456 hive < /home/kylin/update-hive-column-comment.sql
        run_command "Create tpch tables" hive -f /home/kylin/create-all-tables.sql
        hadoop fs -rm -r /user/data/tpch
        hadoop fs -mkdir /user/data/tpch
        hadoop fs -copyFromLocal data/* /user/data/tpch
    fi
fi

touch $START_FLAG
echo "Kylin-Machine is ready for you to remote debug with your local kylin process."
while :
do
    sleep 10
done
