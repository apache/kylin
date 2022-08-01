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

if [ $# != 1 ]
then
    if [[ $# < 2 || $2 != 'DEC' ]]
        then
            echo 'invalid input'
            exit 1
    fi
fi

if [ -z $KYLIN_HOME ];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

if [ -z $MAPR_HOME ];then
    export MAPR_HOME="/opt/mapr"
fi

if [ -z ${kylin_hadoop_conf_dir} ]; then
    export kylin_hadoop_conf_dir=$KYLIN_HOME/hadoop_conf
fi

export KYLIN_KERBEROS_OPTS=""
if [ -f ${KYLIN_HOME}/conf/krb5.conf ];then
    KYLIN_KERBEROS_OPTS="-Djava.security.krb5.conf=${KYLIN_HOME}/conf/krb5.conf"
fi

export SPARK_HOME=$KYLIN_HOME/spark

if [[ -f ${KYLIN_HOME}/conf/kylin-tools-log4j.xml ]]; then
    kylin_tools_log4j="file:${KYLIN_HOME}/conf/kylin-tools-log4j.xml"
    else
    kylin_tools_log4j="file:${KYLIN_HOME}/tool/conf/kylin-tools-log4j.xml"
fi

mkdir -p ${KYLIN_HOME}/logs
result=`java ${KYLIN_KERBEROS_OPTS} -Dlog4j.configurationFile=${kylin_tools_log4j} -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} -Dhdp.version=current -cp "${kylin_hadoop_conf_dir}:${KYLIN_HOME}/lib/ext/*:${KYLIN_HOME}/server/jars/*:${SPARK_HOME}/jars/*" org.apache.kylin.tool.KylinConfigCLI $@ 2>>${KYLIN_HOME}/logs/shell.stderr`

echo "$result"
