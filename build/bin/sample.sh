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

function help(){
    echo "Usage: $0  [--client <hive_client_mode>] [--dir <hdfs_tmp_dir> ] [--params <beeline_params>] [--conf <hive_conf_properties> ]"
    exit 1
}

while [[ $# != 0 ]]; do
    if [[ $# != 1 ]]; then
        if [[ $1 == "--client" ]]; then
            hive_client_mode=$2
        elif [[ $1 == "--dir" ]]; then
            hdfs_tmp_dir=$2
        elif [[ $1 == "--params" ]]; then
            beeline_params=$2
        elif [[ $1 == "--conf" ]]; then
            hive_conf_properties=$2
        else
            help
        fi
        shift
    else
        case $1 in
            --client|--dir|--params|--conf) break
            ;;
            *)
            help
            ;;
        esac
    fi
    shift
done

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh

source ${KYLIN_HOME}/sbin/prepare-hadoop-conf-dir.sh

cd ${KYLIN_HOME}/tool/ssb/data

if [[ -z "${kylin_hadoop_conf_dir}" ]]; then
    hadoop_conf_param=
else
    hadoop_conf_param="--config ${kylin_hadoop_conf_dir}"
fi


#judge hive_client for FusionInsight or default
## FusionInsight platform C60.
if [[ -z $hive_client_mode && -n "$FI_ENV_PLATFORM" ]]; then
    hive_client_mode=beeline
elif [[ -z $hive_client_mode && -z "$FI_ENV_PLATFORM" ]]; then
    hive_client_mode=hive
fi

#set default properties
if [[ -z $hdfs_tmp_dir ]]; then
    hdfs_tmp_dir=/tmp/kylin
fi

echo "Loading sample data into HDFS tmp path: ${hdfs_tmp_dir}/sample_cube/data"

hadoop ${hadoop_conf_param} fs -mkdir -p ${hdfs_tmp_dir}/sample_cube/data

if [[ $? != 0 ]]; then
    quit "Failed to create ${hdfs_tmp_dir}/sample_cube/data. Please make sure the user has right to access ${hdfs_tmp_dir}/sample_cube/data or usage: sample.sh --dir hdfs_tmp_dir"
fi

hadoop ${hadoop_conf_param} fs -put * ${hdfs_tmp_dir}/sample_cube/data/

sample_database=SSB

echo "Going to create sample tables in hive to database "$sample_database" by "$hive_client_mode

if [[ "${hive_client_mode}" == "beeline" ]]; then
    beeline ${hive_conf_properties} ${beeline_params} -e "CREATE DATABASE IF NOT EXISTS "$sample_database
    hive2_url=`expr match "${beeline_params}" '.*\(hive2:.*:[0-9]\{4,6\}\/\)'`
    if [[ -z ${hive2_url} ]]; then
        hive2_url=`expr match "${beeline_params}" '.*\(hive2:.*:[0-9]\{4,6\}\)'`
        beeline_params=${beeline_params/${hive2_url}/${hive2_url}/${sample_database}}
    else
        beeline_params=${beeline_params/${hive2_url}/${hive2_url}${sample_database}}
    fi
    beeline ${hive_conf_properties} --hivevar hdfs_tmp_dir=${hdfs_tmp_dir} ${beeline_params} -f ${KYLIN_HOME}/tool/ssb/create_sample_ssb_tables.sql  || { exit 1; }
elif [[ "${hive_client_mode}" == "hive" ]]; then
    hive ${hive_conf_properties} -e "CREATE DATABASE IF NOT EXISTS "$sample_database
    hive ${hive_conf_properties} --hivevar hdfs_tmp_dir=${hdfs_tmp_dir} --database $sample_database -f ${KYLIN_HOME}/tool/ssb/create_sample_ssb_tables.sql  || { exit 1; }
else
    echo "Now $hive_client_mode is not supported, please use hive or beeline."
fi

echo "Sample hive tables are created successfully; Going to create sample project..."

mkdir -p ${KYLIN_HOME}/sample_project/sample_model/metadata
cp -rf ${KYLIN_HOME}/sample_project/template/* ${KYLIN_HOME}/sample_project/sample_model/metadata

#### Add version info into model
kylin_version=4.0.0.0
echo "kylin version is "$kylin_version
sed -i "s/%default_version%/${kylin_version}/g" ${KYLIN_HOME}/sample_project/sample_model/metadata/_global/project/learn_kylin.json
sed -i "s/%default_version%/${kylin_version}/g" ${KYLIN_HOME}/sample_project/sample_model/metadata/learn_kylin/dataflow/2d07e878-da28-a203-2d2c-185b4c6656f1.json
sed -i "s/%default_version%/${kylin_version}/g" ${KYLIN_HOME}/sample_project/sample_model/metadata/learn_kylin/index_plan/2d07e878-da28-a203-2d2c-185b4c6656f1.json
sed -i "s/%default_version%/${kylin_version}/g" ${KYLIN_HOME}/sample_project/sample_model/metadata/learn_kylin/model_desc/2d07e878-da28-a203-2d2c-185b4c6656f1.json

sed -i "s/%default_version%/${kylin_version}/g" ${KYLIN_HOME}/sample_project/sample_model/metadata/learn_kylin/table/SSB.CUSTOMER.json
sed -i "s/%default_version%/${kylin_version}/g" ${KYLIN_HOME}/sample_project/sample_model/metadata/learn_kylin/table/SSB.DATES.json
sed -i "s/%default_version%/${kylin_version}/g" ${KYLIN_HOME}/sample_project/sample_model/metadata/learn_kylin/table/SSB.P_LINEORDER.json
sed -i "s/%default_version%/${kylin_version}/g" ${KYLIN_HOME}/sample_project/sample_model/metadata/learn_kylin/table/SSB.PART.json
sed -i "s/%default_version%/${kylin_version}/g" ${KYLIN_HOME}/sample_project/sample_model/metadata/learn_kylin/table/SSB.SUPPLIER.json

function turn_on_maintain_mode() {
  echo "enter maintenance mode."
  ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.MaintainModeTool -on -reason 'metastore tool' -hidden-output true
  ret=$?
  if [[ $ret != 0 ]]; then
    exit $ret
  fi
}

function turn_off_maintain_mode() {
  echo "exit maintenance mode."
  ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.MaintainModeTool -off -hidden-output true
}

function printImportResult() {
  error=$1
  if [[ $error == 0 ]]; then
    echo -e "${YELLOW}Sample model is created successfully in project 'learn_kylin'. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
  else
    echo -e "${YELLOW}Sample model is created failed in project 'learn_kylin'. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
  fi
}

function importProject() {
  turn_on_maintain_mode
  ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.SampleProjectTool -dir ${KYLIN_HOME}/sample_project/sample_model/metadata -project learn_kylin -model sample_ssb
  printImportResult $?
  turn_off_maintain_mode
}

importProject
