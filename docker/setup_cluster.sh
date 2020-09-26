#!/bin/bash

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.

SCRIPT_PATH=$(cd `dirname $0`; pwd)
WS_ROOT=`dirname $SCRIPT_PATH`

source ${SCRIPT_PATH}/build_cluster_images.sh

# restart cluster

KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-hadoop.yml down
KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-zookeeper.yml down
KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-metastore.yml down
KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-hive.yml down
sleep 5
# hadoop
KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-hadoop.yml up -d
KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-zookeeper.yml up -d
KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-metastore.yml up -d
KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-hive.yml up -d


if [ $KERBEROS == "yes" ]; then
  KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-kerberos.yml down
  sleep 2
  KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-kerberos.yml up -d
fi

if [ $LDAP == "yes" ]; then
  KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-ldap.yml down
  sleep 2
  KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-ldap.yml up -d
fi

if [ $KAFKA == "yes" ]; then
  KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-kafka.yml down
  sleep 2
  KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-kafka.yml up -d
fi


if [ $CLUSTER_MODE == "write" ]; then
  KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-kylin-write.yml down
  if [ $HBASE == "yes" ]; then
    KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-hbase.yml down
    sleep 2
    KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-hbase.yml up -d
  fi
  KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-kylin-write.yml up -d
fi

# restart cluster
if [ $CLUSTER_MODE == "write-read" ]; then
  KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/read/docker-compose-zookeeper.yml down
  KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/read/docker-compose-hadoop.yml down
  KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-kylin-write-read.yml down
  sleep 5
  KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/read/docker-compose-zookeeper.yml up -d
  KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/read/docker-compose-hadoop.yml up -d

  if [ $HBASE == "yes" ]; then
    KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/read/docker-compose-hbase.yml down
    sleep 2
    KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/read/docker-compose-hbase.yml up -d
  fi

  KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-kylin-write-read.yml up -d
fi

