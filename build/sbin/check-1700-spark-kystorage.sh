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

#title=Checking Spark Availability

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
source ${KYLIN_HOME}/sbin/init-kerberos.sh

## init Kerberos if needed
initKerberosIfNeeded

columnarEnabled=`${dir}/get-properties.sh kylin.storage.columnar.start-own-spark`
if [[ -z "$columnarEnabled" ]]; then
    columnarEnabled="true"
fi
[[ "${columnarEnabled}" == "true" ]] || exit 3

serverMode=`${dir}/get-properties.sh kylin.server.mode`
# skip check in job node or master node
[[ "${serverMode}" != "job" ]] || exit 3
[[ "${serverMode}" != "master" ]] || exit 3


echo "Checking spark home..."

# check SPARK_HOME

[[ -z ${SPARK_HOME} ]] || [[ ${SPARK_HOME} == ${KYLIN_HOME}/spark ]] || echo "${CHECKENV_REPORT_PFX}`setColor 32 Important!` Current SPARK_HOME is set: ${SPARK_HOME}, please don't risk it."

# prepare spark
source ${KYLIN_HOME}/sbin/do-check-and-prepare-spark.sh

echo "Checking spark executor config..."

override_file="${KYLIN_HOME}/conf/kylin.properties"

if [ ! -f ${override_file} ]; then
    echo "${override_file} not exist. Please check"
    exit 1
fi

key_executor_cores="kylin.storage.columnar.spark-conf.spark.executor.cores"
key_executor_memory="kylin.storage.columnar.spark-conf.spark.executor.memory"
key_executor_instance="kylin.storage.columnar.spark-conf.spark.executor.instances"

mkdir -p ${KYLIN_HOME}/logs
saveFileName=${KYLIN_HOME}/logs/cluster.info
${KYLIN_HOME}/sbin/bootstrap.sh io.kyligence.kap.tool.setup.KapGetClusterInfo ${saveFileName}

if [ $? != 0 ]; then
    echo "${CHECKENV_REPORT_PFX}WARN: Failed to get cluster' info, skip the spark config suggestion."
    exit 3
fi

#def constant var
yarn_available_cores=`getValueByKey availableVirtualCores ${saveFileName}`
yarn_available_memory=`getValueByKey availableMB ${saveFileName}`
spark_executor_cores=`${dir}/get-properties.sh ${key_executor_cores}`
spark_executor_memory=`${dir}/get-properties.sh ${key_executor_memory}`
spark_executor_instance=`${dir}/get-properties.sh ${key_executor_instance}`

if [ -z ${yarn_available_cores} ]; then
    echo "${CHECKENV_REPORT_PFX}WARN: Cannot get Yarn RM's cores info, skip the spark config suggestion."
    exit 3
fi

if [ -z ${yarn_available_memory} ]; then
    echo "${CHECKENV_REPORT_PFX}WARN: Cannot get Yarn RM's memory info, skip the spark config suggestion."
    exit 3
fi

echo "${key_executor_cores}=`setColor 36 ${spark_executor_cores}`"
echo "${key_executor_memory}=`setColor 36 ${spark_executor_memory}`"
echo "${CHECKENV_REPORT_PFX}Available YARN RM cores: ${yarn_available_cores}"
echo "${CHECKENV_REPORT_PFX}Available YARN RM memory: ${yarn_available_memory}M"
unit=${spark_executor_memory: -1}
unit=$(echo ${unit} | tr [a-z] [A-Z])

if [ "${unit}" == "M" ];then
    spark_executor_memory=${spark_executor_memory%?}
elif [ "${unit}" == "G" ];then
    spark_executor_memory=`expr 1024 \* ${spark_executor_memory%?}`
else
    quit "Unrecognized memory unit: ${unit} in ${spark_executor_memory} in kylin.properties";
fi

if [[ -z "$TDH_CLIENT_HOME" ]]; then
  [[ ${yarn_available_cores} -gt ${spark_executor_cores} ]] || quit "In kylin.properties, ${key_executor_cores} is set to ${spark_executor_cores}, which is greater than Yarn's available cores: ${yarn_available_cores}, please correct it."
  [[ ${yarn_available_memory} -gt ${spark_executor_memory} ]] || quit "In kylin.properties, ${key_executor_memory} is set to ${spark_executor_memory}M, which is more than Yarn's available memory: ${yarn_available_memory}M, please correct it."
fi

ins1=`expr ${yarn_available_memory} / ${spark_executor_memory}`
ins2=`expr ${yarn_available_cores} / ${spark_executor_cores}`

if [ ${ins1} -lt ${ins2} ]; then
    recommend_max=${ins1}
else
    recommend_max=${ins2}
fi

recommend_min=`expr ${recommend_max} / 5`

if [ -z ${spark_executor_instance} ]; then
    echo "${CHECKENV_REPORT_PFX}`setColor 31 WARN:` ${key_executor_instance} is not set."
elif [ -z "$TDH_CLIENT_HOME" ] && [ ${spark_executor_instance} -gt ${recommend_max} ]; then
    quit "Spark executor instances: ${spark_executor_instance} is configured in kylin.properties shouldn't beyond maximum: ${recommend_max} in theory."
elif [ ${recommend_min} -gt ${spark_executor_instance} ]; then
    echo "${CHECKENV_REPORT_PFX}`setColor 31 WARN:` Only ${spark_executor_instance} Spark executor instances available, your query performance might be affected. It's recommended to set it to be ${recommend_min} - ${recommend_max} via configuring parameter ${key_executor_instance} in kylin.properties."
else
    echo "${CHECKENV_REPORT_PFX}The max executor instances can be `setColor 36 ${recommend_max}`"
    echo "${CHECKENV_REPORT_PFX}The current executor instances is `setColor 36 ${spark_executor_instance}`"
fi

echo "Testing spark task..."
${KYLIN_HOME}/sbin/spark-test.sh test
[[ $? == 0 ]] || quit "Test of spark failed, please check the logs/check-env.out for the details."
