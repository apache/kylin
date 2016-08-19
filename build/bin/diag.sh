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

# We should set KYLIN_HOME here for multiple tomcat instances that are on the same node.
# In addition, we should set a KYLIN_HOME for the global use as normal.
KYLIN_HOME=`dirname $0`/..
export KYLIN_HOME=`cd "$KYLIN_HOME"; pwd`
dir="$KYLIN_HOME/bin"

source ${dir}/check-env.sh
mkdir -p ${KYLIN_HOME}/logs

tomcat_root=${dir}/../tomcat
export tomcat_root

if [ $# -eq 1 ] || [ $# -eq 2 ]
then
    patient="$1"
    if [ -z "$patient" ]
    then
        echo "You need to specify a Project or Job Id for diagnosis."
        exit 1
    fi
    destDir="$2"
    if [ -z "$destDir" ]
    then
        destDir="$KYLIN_HOME/diagnosis_dump/"
        mkdir -p $destDir
    fi

    source ${dir}/find-hive-dependency.sh

    #retrive $KYLIN_EXTRA_START_OPTS
    if [ -f "${dir}/setenv-tool.sh" ]
        then source ${dir}/setenv-tool.sh
    fi

    mkdir -p ${KYLIN_HOME}/ext
    export HBASE_CLASSPATH_PREFIX=${KYLIN_HOME}/conf:${KYLIN_HOME}/lib/*:${KYLIN_HOME}/tool/*:${KYLIN_HOME}/ext/*:${HBASE_CLASSPATH_PREFIX}
    export HBASE_CLASSPATH=${HBASE_CLASSPATH}:${hive_dependency}

    if [ ${#patient} -eq 36 ]; then
        hbase ${KYLIN_EXTRA_START_OPTS} \
        -Dlog4j.configuration=kylin-server-log4j.properties \
        -Dcatalina.home=${tomcat_root} \
        org.apache.kylin.tool.JobDiagnosisInfoCLI \
        -jobId $patient \
        -destDir $destDir || exit 1
    else
        hbase ${KYLIN_EXTRA_START_OPTS} \
        -Dlog4j.configuration=kylin-server-log4j.properties \
        -Dcatalina.home=${tomcat_root} \
        org.apache.kylin.tool.DiagnosisInfoCLI \
        -project -all \
        -destDir $destDir || exit 1
    fi

    exit 0
else
    echo "usage: diag.sh Project|JobId [target_path]"
    exit 1
fi