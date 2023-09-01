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
    echo "[$STEP] succeed."
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

run_command "Start MySQL" service mysql start
if [ ! -f "/home/kylin/first_run" ]
then
    run_command "Create Database" mysql -uroot -p123456 -e "CREATE DATABASE IF NOT EXISTS kylin default charset utf8mb4 COLLATE utf8mb4_general_ci;"
    run_command "Init Hive" schematool -initSchema -dbType mysql
    run_command "Format HDFS" hdfs namenode -format
fi

run_command "Start HDFS [NameNode]" hdfs --daemon start namenode
run_command "Start HDFS [DataNode]" hdfs --daemon start datanode

# start yarn
run_command "Start Yarn [ResourceManager]" yarn --daemon start resourcemanager
run_command "Start Yarn [NodeManager]" yarn --daemon start nodemanager

# start job history server
# run_command "" mapred --daemon start historyserver

run_command "Start Zookeeper" "$ZK_HOME"/bin/zkServer.sh start

sleep 10s

if [ ! -f $START_FLAG ]
then
    check_and_monitor_status "HDFS Usability" hadoop fs -mkdir /tmp
    mkdir -p "$KYLIN_HOME"/logs
    hdfs dfs -mkdir -p /kylin
    run_command "Prepare sample data" "$KYLIN_HOME"/bin/sample.sh
fi


run_command "Start Kylin Instance" "$KYLIN_HOME"/bin/kylin.sh -v start

check_and_monitor_status "Check Env Script" ls $KYLIN_HOME/bin/check-env-bypass
check_and_monitor_status "Kylin Instance" cat "$KYLIN_HOME"/logs/kylin.log | grep -c "Initialized Spark"

touch $START_FLAG
echo "Kylin service is already available for you to preview."
while :
do
    sleep 10
done
