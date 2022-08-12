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

#title=Checking Kylin Config

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking Kylin Config..."

if [ -z $KYLIN_HOME ];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

export SPARK_HOME=$KYLIN_HOME/spark

if [[ -f ${KYLIN_HOME}/conf/kylin-tools-log4j.xml ]]; then
    kylin_tools_log4j="file:${KYLIN_HOME}/conf/kylin-tools-log4j.xml"
    else
    kylin_tools_log4j="file:${KYLIN_HOME}/tool/conf/kylin-tools-log4j.xml"
fi

mkdir -p ${KYLIN_HOME}/logs
error_config=`java -Dlog4j.configurationFile=${kylin_tools_log4j} -cp "${KYLIN_HOME}/lib/ext/*:${KYLIN_HOME}/server/jars/*:${SPARK_HOME}/jars/*" org.apache.kylin.tool.KylinConfigCheckCLI 2>>${KYLIN_HOME}/logs/shell.stderr`


if [[ -n $error_config ]]; then
    quit "Error config: ${error_config} in kylin configuration file"
fi
