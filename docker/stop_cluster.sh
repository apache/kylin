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

KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-kylin-write.yml down
KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-kylin-write-read.yml down

KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/read/docker-compose-zookeeper.yml down
KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/read/docker-compose-hadoop.yml down
KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/read/docker-compose-hbase.yml down


KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-kafka.yml down
KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-hbase.yml down

KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-hive.yml down
KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-zookeeper.yml down

KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-kerberos.yml down
# KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-ldap.yml down
KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/others/docker-compose-metastore.yml down

KYLIN_WS=${WS_ROOT} docker-compose -f ${SCRIPT_PATH}/docker-compose/write/docker-compose-hadoop.yml down

# clean data
rm -rf ${SCRIPT_PATH}/docker-compose/write/data/*
rm -rf ${SCRIPT_PATH}/docker-compose/read/data/*
rm -rf ${SCRIPT_PATH}/docker-compose/others/data/*
