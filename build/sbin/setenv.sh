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

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh

if [[ -d "/data/external-catalog" ]];then
    plugin_version=`${KYLIN_HOME}/bin/get-properties.sh kylin.datasource.external-catalog.version`
    cp -rf /data/external-catalog/$plugin_version/*.jar ${KYLIN_HOME}/spark/jars/
    source /data/external-catalog/$plugin_version/setenv.sh
fi

if [[ `isValidJavaVersion` == "false" ]]; then
    quit "ERROR: Java 1.8 or above is required for Kyligence Enterprise"
fi

if [[ -f "${KYLIN_HOME}/conf/setenv.sh" ]]; then
    source ${KYLIN_HOME}/conf/setenv.sh
fi

if [[ -d "/data/external-jars" ]];then
    cp -rf /data/external-jars/*.jar ${KYLIN_HOME}/spark/jars/
fi

export JAVA_VM_XMS=${JAVA_VM_XMS:-1g}
export JAVA_VM_XMX=${JAVA_VM_XMX:-8g}

export JAVA_VM_TOOL_XMS=${JAVA_VM_TOOL_XMS:-${JAVA_VM_XMS}}
export JAVA_VM_TOOL_XMX=${JAVA_VM_TOOL_XMX:-${JAVA_VM_XMX}}

export KYLIN_EXTRA_START_OPTS=""
export KYLIN_JVM_SETTINGS=${KYLIN_JVM_SETTINGS:-"-server -Xms${JAVA_VM_XMS} -Xmx${JAVA_VM_XMX} -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=16m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark  -Xloggc:${KYLIN_HOME}/logs/kylin.gc.%p  -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=64M -XX:-OmitStackTraceInFastThrow -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector -DAsyncLogger.RingBufferSize=8192"}

# add some must have settings
if [[ ${KYLIN_JVM_SETTINGS} != *"-DAsyncLogger.RingBufferSize="* ]]
then
 export KYLIN_JVM_SETTINGS="${KYLIN_JVM_SETTINGS} -DAsyncLogger.RingBufferSize=8192"
fi

# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode. Tune the variable down to prevent vmem explosion.
# See HADOOP-7154.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

verbose "KYLIN_JVM_SETTINGS is ${KYLIN_JVM_SETTINGS}"
KYLIN_EXTRA_START_OPTS="${KYLIN_JVM_SETTINGS} ${KYLIN_EXTRA_START_OPTS}"

if [[ ! -z "${KYLIN_DEBUG_SETTINGS}" ]]
then
    verbose "KYLIN_DEBUG_SETTINGS is ${KYLIN_DEBUG_SETTINGS}"
    KYLIN_EXTRA_START_OPTS="${KYLIN_DEBUG_SETTINGS} ${KYLIN_EXTRA_START_OPTS}"
else
    verbose "KYLIN_DEBUG_SETTINGS is not set, will not enable remote debuging"
fi

if [[ ! -z "${KYLIN_LD_LIBRARY_SETTINGS}" ]]
then
    verbose "KYLIN_LD_LIBRARY_SETTINGS is ${KYLIN_LD_LIBRARY_SETTINGS}"
    KYLIN_EXTRA_START_OPTS="${KYLIN_LD_LIBRARY_SETTINGS} ${KYLIN_EXTRA_START_OPTS}"
else
    verbose "KYLIN_LD_LIBRARY_SETTINGS is not set, it is okay unless you want to specify your own native path"
fi
