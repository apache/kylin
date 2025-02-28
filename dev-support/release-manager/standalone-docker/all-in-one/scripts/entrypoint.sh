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

FLAG_INITIALIZED="/home/kylin/initialized"
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

function check_hive_port() {
    local port=$1
    if [[ $(lsof -i :$port) == *"LISTEN"* ]]; then
        return 0
    else
        return 1
    fi
}

# clean pid files
rm -f /tmp/*.pid
rm -rf /data/zookeeper/*
rm -f /data/zookeeper/zookeeper_server.pid

##############################################

# start ssh
run_command "Start SSH server" /etc/init.d/ssh start

# start mysql
run_command "Start MySQL" service mysql start

# env init
if [ ! -f $FLAG_INITIALIZED ]; then
    run_command "Create Database kylin" mysql -uroot -p123456 -e "CREATE DATABASE IF NOT EXISTS kylin default charset utf8mb4 COLLATE utf8mb4_general_ci;"
    run_command "Create Database hive3" mysql -uroot -p123456 -e "CREATE DATABASE IF NOT EXISTS hive3 default charset utf8mb4 COLLATE utf8mb4_general_ci;"
    run_command "Init Hive" schematool -initSchema -dbType mysql
    run_command "Format HDFS" hdfs namenode -format
fi

# start zookeeper
run_command "Start Zookeeper" "$ZOOKEEPER_HOME"/bin/zkServer.sh start

# start hadoop
run_command "Start Hadoop" "$HADOOP_HOME"/sbin/start-all.sh

# start job history server
run_command "Start History Server" "$HADOOP_HOME"/sbin/start-historyserver.sh

# start hive metastore & hiveserver2
run_command "Start Hive metastore" "$HIVE_HOME"/bin/start-hivemetastore.sh
check_and_monitor_status "Check Hive metastore" check_hive_port 9083
run_command "Start Hive server" "$HIVE_HOME"/bin/start-hiveserver2.sh
check_and_monitor_status "Check Hive server" check_hive_port 10000

sleep 10

# pre-running initializing
if [ ! -f $FLAG_INITIALIZED ]
then
    mkdir -p "$KYLIN_HOME"/logs
    hdfs dfs -mkdir -p /kylin
    run_command "Prepare sample data" "$KYLIN_HOME"/bin/sample.sh
fi

# start kylin
run_command "Kylin ENV bypass" touch $KYLIN_HOME/bin/check-env-bypass
run_command "Start Kylin Instance" "$KYLIN_HOME"/bin/kylin.sh -v start

check_and_monitor_status "Check Env Script" ls $KYLIN_HOME/bin/check-env-bypass
check_and_monitor_status "Kylin Instance" grep -c "Initialized Spark" $KYLIN_HOME/logs/kylin.log

touch $FLAG_INITIALIZED
echo "Kylin service is already available for you to preview."

# keep docker running
sleep infinity
