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

SCRIPT_PATH=$(cd `dirname $0`; pwd)
WS_ROOT=`dirname $SCRIPT_PATH`

source ${SCRIPT_PATH}/header.sh

docker build -t apachekylin/kylin-ci-hadoop-base:hadoop_${HADOOP_VERSION} --build-arg HADOOP_VERSION=${HADOOP_VERSION} ./dockerfile/cluster/base
docker build -t apachekylin/kylin-ci-hadoop-namenode:hadoop_${HADOOP_VERSION} --build-arg HADOOP_VERSION=${HADOOP_VERSION} --build-arg HADOOP_WEBHDFS_PORT=${HADOOP_WEBHDFS_PORT} ./dockerfile/cluster/namenode
docker build -t apachekylin/kylin-ci-hadoop-datanode:hadoop_${HADOOP_VERSION} --build-arg HADOOP_VERSION=${HADOOP_VERSION} --build-arg HADOOP_DN_PORT=${HADOOP_DN_PORT} ./dockerfile/cluster/datanode
docker build -t apachekylin/kylin-ci-hadoop-resourcemanager:hadoop_${HADOOP_VERSION} --build-arg HADOOP_VERSION=${HADOOP_VERSION} ./dockerfile/cluster/resourcemanager
docker build -t apachekylin/kylin-ci-hadoop-nodemanager:hadoop_${HADOOP_VERSION} --build-arg HADOOP_VERSION=${HADOOP_VERSION} ./dockerfile/cluster/nodemanager
docker build -t apachekylin/kylin-ci-hadoop-historyserver:hadoop_${HADOOP_VERSION} --build-arg HADOOP_VERSION=${HADOOP_VERSION} ./dockerfile/cluster/historyserver

docker build -t apachekylin/kylin-ci-hive:hive_${HIVE_VERSION}_hadoop_${HADOOP_VERSION} \
  --build-arg HIVE_VERSION=${HIVE_VERSION} \
  --build-arg HADOOP_VERSION=${HADOOP_VERSION} \
  ./dockerfile/cluster/hive

if [ $ENABLE_HBASE == "yes" ]; then
  docker build -t apachekylin/kylin-ci-hbase-base:hbase_${HBASE_VERSION} --build-arg HBASE_VERSION=${HBASE_VERSION} ./dockerfile/cluster/hbase
  docker build -t apachekylin/kylin-ci-hbase-master:hbase_${HBASE_VERSION} --build-arg HBASE_VERSION=${HBASE_VERSION} ./dockerfile/cluster/hmaster
  docker build -t apachekylin/kylin-ci-hbase-regionserver:hbase_${HBASE_VERSION} --build-arg HBASE_VERSION=${HBASE_VERSION} ./dockerfile/cluster/hregionserver
fi

if [ $ENABLE_KERBEROS == "yes" ]; then
  docker build -t apachekylin/kylin-ci-kerberos:latest ./dockerfile/cluster/kerberos
fi

if [ $ENABLE_LDAP == "yes" ]; then
  docker pull osixia/openldap:1.3.0
fi

if [ $ENABLE_KAFKA == "yes" ]; then
  docker pull bitnami/kafka:2.0.0
fi

docker build -t apachekylin/kylin-ci-client:hadoop_${HADOOP_VERSION}_hive_${HIVE_VERSION}_hbase_${HBASE_VERSION} \
  --build-arg HIVE_VERSION=${HIVE_VERSION} \
  --build-arg HADOOP_VERSION=${HADOOP_VERSION} \
  --build-arg HBASE_VERSION=${HBASE_VERSION} \
  ./dockerfile/cluster/client
