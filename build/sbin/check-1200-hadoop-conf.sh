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

#title=Checking Hadoop Configuration

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
source ${KYLIN_HOME}/sbin/init-kerberos.sh
source ${KYLIN_HOME}/sbin/prepare-hadoop-env.sh

## init Kerberos if needed
initKerberosIfNeeded

source ${KYLIN_HOME}/sbin/prepare-hadoop-conf-dir.sh

echo "Checking hadoop conf dir..."

[[ -z "${kylin_hadoop_conf_dir}" ]] && quit "ERROR: Failed to find Hadoop config dir, please set kylin_hadoop_conf_dir."

prepare_hadoop_conf_jars

# this is the very first check, apply -v to print verbose classpath in check-env log
${KYLIN_HOME}/sbin/bootstrap.sh -v org.apache.kylin.tool.hadoop.CheckHadoopConfDir "${kylin_hadoop_conf_dir}"

# CheckHadoopConfDir will print the last error message
[[ $? == 0 ]] || quit "ERROR: Check HADOOP_CONF_DIR failed. Please correct hadoop configurations."

function getSourceFile() {

    export FI_ENV_PLATFORM=

    ## FusionInsight platform C60.
    if [ -n "$BIGDATA_HOME" ]
    then
        FI_ENV_PLATFORM=$BIGDATA_HOME
    fi

    ## FusionInsight platform C70.
    if [ -n "$BIGDATA_CLIENT_HOME" ]
    then
        FI_ENV_PLATFORM=$BIGDATA_CLIENT_HOME
    fi

    if [[ -n $FI_ENV_PLATFORM ]]; then
        if [[ $1 == "hive-site.xml" ]]; then
            echo "${FI_ENV_PLATFORM}/Hive/config/hive-site.xml"
            return
        fi
        echo "${FI_ENV_PLATFORM}/HDFS/hadoop/etc/hadoop/$1"
        return
    fi

    if [[ -d "/etc/hadoop/conf" ]]; then
        if [[ $1 == "hive-site.xml" ]]; then
            echo "/etc/hive/conf/hive-site.xml"
            return
        fi
        echo "/etc/hadoop/conf/$1"
        return
    fi
}

if [[ ! -f ${kylin_hadoop_conf_dir}/core-site.xml ]]; then
    source_file=$(getSourceFile "core-site.xml")
    [[ -z ${source_file} ]] || quit "core-site.xml does not exist in ${kylin_hadoop_conf_dir}, please copy it from ${source_file}"
fi

if [[ ! -f ${kylin_hadoop_conf_dir}/hdfs-site.xml ]]; then
    source_file=$(getSourceFile "hdfs-site.xml")
    [[ -z ${source_file} ]] || quit "hdfs-site.xml does not exist in ${kylin_hadoop_conf_dir}, please copy it from ${source_file}"
fi

if [[ ! -f ${kylin_hadoop_conf_dir}/yarn-site.xml ]]; then
    source_file=$(getSourceFile "yarn-site.xml")
    [[ -z ${source_file} ]] || quit "yarn-site.xml does not exist in ${kylin_hadoop_conf_dir}, please copy it from ${source_file}"
fi

if [[ ! -f ${kylin_hadoop_conf_dir}/hive-site.xml ]]; then
    source_file=$(getSourceFile "hive-site.xml")
    [[ -z ${source_file} ]] || quit "hive-site.xml does not exist in ${kylin_hadoop_conf_dir}, please copy it from ${source_file}"
fi

if [[ $(is_kap_kerberos_enabled) == 1 && ! -f ${kylin_hadoop_conf_dir}/$KYLIN_KRB5CONF ]]; then
    quit "krb5.conf does not exist in ${kylin_hadoop_conf_dir}, please copy it from ${KYLIN_HOME}/conf/${KYLIN_KRB5CONF}"
fi
