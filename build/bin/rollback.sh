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

if [ -z $KYLIN_HOME ];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi


function help(){
    echo "Usage: $0  [--time <travel_time(must required)>] [--project <project_name>] [--skip-check-data <skip_check_storage_data> ]"
    exit 1
}

function printBackupResult() {
    error=$1
    if [[ $error == 0 ]]; then
        echo "rollback success"
    else
        echo -e "rollback failed please check logs/shell.stderr and  logs/shell.stdout for reason"
    fi
}


mkdir -p ${KYLIN_HOME}/logs
ERR_LOG=${KYLIN_HOME}/logs/shell.stderr
OUT_LOG=${KYLIN_HOME}/logs/shell.stdout

PROJECT_SECTION=
SKIP_CHECK_DATA_SECTION=
TIME=
function main() {
    while [[ $# != 0 ]]; do
        if [[ $1 == "-t" || $1 == "--time" ]]; then
            TIME="$2"
        elif [[ $1 == "-p" || $1 == "--project"  ]]; then
            PROJECT_SECTION="-project $2"
        elif [[ $1 == "--skip-check-data" ]]; then
            SKIP_CHECK_DATA_SECTION="-skipCheckData true"
        fi
        shift
    done
    if [[ -z $TIME ]]; then
      echo "Specify the travel time(must required)"
        help
    fi
    echo $PROJECT_SECTION
    echo $SKIP_CHECK_DATA_SECTION
    echo "org.apache.kylin.tool.RollbackTool -time '$TIME' $PROJECT_SECTION $SKIP_CHECK_DATA_SECTION"

    source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh "$@"
    version=`cat ${KYLIN_HOME}/VERSION | awk '{print $3}'`
    ${KYLIN_HOME}/sbin/rotate-logs.sh "$@"

    if [ "$1" == "-v" ]; then
        shift
    fi

    source ${KYLIN_HOME}/sbin/setenv.sh
    source ${KYLIN_HOME}/sbin/prepare-hadoop-conf-dir.sh
    export SPARK_HOME=${KYLIN_HOME}/spark

    java -Xms${JAVA_VM_TOOL_XMS} -Xmx${JAVA_VM_TOOL_XMX} -cp "${kylin_hadoop_conf_dir}:${KYLIN_HOME}/lib/ext/*:${KYLIN_HOME}/server/jars/*:${SPARK_HOME}/jars/*" io.kyligence.kap.tool.RollbackTool -time "$TIME"  $PROJECT_SECTION $SKIP_CHECK_DATA_SECTION 2>>${ERR_LOG}  | tee -a ${OUT_LOG}

    printBackupResult ${PIPESTATUS[0]}
}

main "$@"
