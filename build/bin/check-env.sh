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

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/find-hadoop-conf-dir.sh

if [ -z "${kylin_hadoop_conf_dir}" ]; then
    hadoop_conf_param=
else
    hadoop_conf_param="--config ${kylin_hadoop_conf_dir}"
fi

if [ -z "$KYLIN_HOME" ]
then
    quit 'Please make sure KYLIN_HOME has been set'
else
    echo "KYLIN_HOME is set to ${KYLIN_HOME}"
fi

if [ -z "$(command -v hbase version)" ]
then
    quit "Please make sure the user has the privilege to run hbase shell"
fi

if [ -z "$(command -v hive --version)" ]
then
    quit "Please make sure the user has the privilege to run hive shell"
fi

if [ -z "$(command -v hadoop version)" ]
then
    quit "Please make sure the user has the privilege to run hadoop shell"
fi

WORKING_DIR=`bash $KYLIN_HOME/bin/get-properties.sh kylin.env.hdfs-working-dir`
if [ -z "$WORKING_DIR" ]
then
    quit "Please set kylin.env.hdfs-working-dir in kylin.properties"
fi

hadoop ${hadoop_conf_param} fs -mkdir -p $WORKING_DIR
if [ $? != 0 ]
then
    quit "Failed to create $WORKING_DIR. Please make sure the user has right to access $WORKING_DIR"
fi

hadoop ${hadoop_conf_param} fs -mkdir -p $WORKING_DIR/spark-history
if [ $? != 0 ]
then
    quit "Failed to create $WORKING_DIR/spark-history. Please make sure the user has right to access $WORKING_DIR"
fi