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

#title=Checking Spark Dir

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
source ${KYLIN_HOME}/sbin/prepare-hadoop-conf-dir.sh
source ${KYLIN_HOME}/sbin/init-kerberos.sh

## init Kerberos if needed
initKerberosIfNeeded

echo "Checking Spark directory..."

function checkDirExistAndPermission() {
    directory=$1
    message=$2

    hadoop ${hadoop_conf_param} fs -test -d ${directory} || hadoop ${hadoop_conf_param} fs -mkdir -p ${directory}

    if [[ $message == "hive-scratch" ]]; then
        # rwxrwxrw-
        hadoop ${hadoop_conf_param} fs -chmod -R 777 ${directory}
    fi

    # test write permission
    RANDNAME=chkenv__${RANDOM}
    TEST_FILE=${directory}/${RANDNAME}

    touch ./${RANDNAME}
    hadoop ${hadoop_conf_param} fs -put -f ./${RANDNAME} ${TEST_FILE} || quit "ERROR: Have no permission to create/modify file in ${message} directory '${directory}'. Please grant permission to current user."

    rm -f ./${RANDNAME}
    hadoop ${hadoop_conf_param} fs -rm -skipTrash ${TEST_FILE} > /dev/null
}

if [ -z "${kylin_hadoop_conf_dir}" ]; then
    hadoop_conf_param=
else
    hadoop_conf_param="--config ${kylin_hadoop_conf_dir}"
fi

# check spark history directory
spark_log_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.engine.spark-conf.spark.history.fs.logDirectory`
spark_eventlog_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.engine.spark-conf.spark.eventLog.dir`

checkDirExistAndPermission ${spark_log_dir} "spark history log"

if [[ ${spark_log_dir} != ${spark_eventlog_dir} ]]; then
    checkDirExistAndPermission ${spark_eventlog_dir} "spark history event log"
fi

sparder_log_dir=`$KYLIN_HOME/bin/get-properties.sh kap.storage.columnar.spark-conf.spark.history.fs.logDirectory`
sparder_eventlog_dir=`$KYLIN_HOME/bin/get-properties.sh kap.storage.columnar.spark-conf.spark.eventLog.dir`

checkDirExistAndPermission ${sparder_log_dir} "sparder history log"

if [[ ${sparder_log_dir} != ${sparder_eventlog_dir} ]]; then
    checkDirExistAndPermission ${sparder_eventlog_dir} "sparder history event log"
fi

# check hive-scratch directory
engine_config_hive_scratch_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.engine.spark-conf.spark.hadoop.hive.exec.scratchdir`
storage_config_hive_scratch_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.spark-conf.spark.hadoop.hive.exec.scratchdir`

checkDirExistAndPermission ${engine_config_hive_scratch_dir} "hive-scratch"

if [[ ${engine_config_hive_scratch_dir} != ${storage_config_hive_scratch_dir} ]]; then
    checkDirExistAndPermission ${storage_config_hive_scratch_dir} "hive-scratch"
fi