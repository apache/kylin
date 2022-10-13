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

function help() {
  echo "Usage: prepare-flat-table.sh <projects> [--update]"
  echo
  echo "<projects>          Optional, Specify the projects to move flat table directory from"
  echo "                    readCluster to writeCluster. Use ',' as separator."
  echo "                    If projects is empty, all projects are fetched by default."
  echo
  echo "--update            Optional, whether to update an existing flat table directory."
  echo "Note:               Not support [--overwrite], please execute manually if necessary."
  exit 1
}

if [[ "$1" == "--help" ]] || [[ "$1" == "--h" ]] || [[ "$1" == "-h" ]] || [[ "$1" == "-help" ]]; then
  help
  exit 0
fi

projects_or_update=$1
projects_to_handle=
update_flag="false"
projects_arr=

# 1. script with no args
# 2. script --update
# 3. script project1,project2,...
# 4. script project1,project2,... --update
if [[ -n ${projects_or_update} ]]; then
  if [[ "${projects_or_update}" == "--update" ]]; then
    update_flag="true"
    shift
  else
    projects_to_handle=${projects_or_update}
    shift
    update=$1
    if [[ -n ${update} ]]; then
      if [[ "${update}" == "--update" ]]; then
        update_flag="true"
        shift
      else
        help
        exit 0
      fi
    fi
  fi
fi

source "$(cd -P -- "$(dirname -- "$0")" && pwd -P)"/header.sh
source "${KYLIN_HOME}"/sbin/init-kerberos.sh

prepare_flat_table_log=${KYLIN_HOME}/logs/prepare-flatTable.log

KYLIN_METADATA_URL_IDENTIFIER=$("${KYLIN_HOME}"/bin/get-properties.sh kylin.metadata.url.identifier)

READ_CLUSTER_WORKING_DIR=$("${KYLIN_HOME}"/bin/get-properties.sh kylin.env.hdfs-working-dir)
READ_CLUSTER_FLAT_TABLE_DIR=${READ_CLUSTER_WORKING_DIR}/${KYLIN_METADATA_URL_IDENTIFIER}

WRITE_CLUSTER_WORKING_DIR=$("${KYLIN_HOME}"/bin/get-properties.sh kylin.env.hdfs-write-working-dir)
WRITE_CLUSTER_FLAT_TABLE_DIR=${WRITE_CLUSTER_WORKING_DIR}/${KYLIN_METADATA_URL_IDENTIFIER}

TIME=$(date "+%Y-%m-%d %H:%M:%S")
echo "INFO : [Operation: move flat table start] user: $(whoami), time:${TIME}" | tee -a "${prepare_flat_table_log}"
echo "Preparing to move flat table from ${READ_CLUSTER_FLAT_TABLE_DIR} to ${WRITE_CLUSTER_FLAT_TABLE_DIR}" | tee -a "${prepare_flat_table_log}"

function printInfo() {
  result=$1
  print_msg=$2
  if [[ ${result} == 0 ]]; then
    echo "${print_msg}" succeed
  else
    echo "${print_msg}" failed
  fi
}

function moveFlatTable() {
  for project in ${projects_arr}; do
    project="${project//,/}"
    echo "Ready to move project: ${project}"
    if [ "${update_flag}" == "true" ]; then
      hadoop distcp -update "${READ_CLUSTER_FLAT_TABLE_DIR}"/"${project}"/flat_table "${WRITE_CLUSTER_FLAT_TABLE_DIR}"/"${project}"/flat_table
      printInfo $? "hadoop -update distcp ${READ_CLUSTER_FLAT_TABLE_DIR}/${project}/flat_table ${WRITE_CLUSTER_FLAT_TABLE_DIR}/${project}/flat_table"
    else
      hadoop distcp "${READ_CLUSTER_FLAT_TABLE_DIR}"/"${project}"/flat_table "${WRITE_CLUSTER_FLAT_TABLE_DIR}"/"${project}/flat_table"
      printInfo $? "hadoop distcp ${READ_CLUSTER_FLAT_TABLE_DIR}/${project}/flat_table ${WRITE_CLUSTER_FLAT_TABLE_DIR}/${project}/flat_table"
    fi
  done
}

# If no project is passed in, all projects will be retrieved by default, multiple projects are supported, separated by ","
# If the project already has a flat_table directory, it is skipped by default unless updated by passing update
if [[ -z ${projects_to_handle} ]]; then
  echo "Ready to get all projects..." | tee -a "${prepare_flat_table_log}"
  projects_arr=$("${KYLIN_HOME}"/bin/kylin.sh org.apache.kylin.tool.ProjectTool | tail -1)
else
  ## init Kerberos if needed
  initKerberosIfNeeded
  projects_arr="${projects_to_handle//,/, }"
fi
echo "project_list: ${projects_arr[*]}" | tee -a "${prepare_flat_table_log}"
moveFlatTable "${projects_arr}" | tee -a "${prepare_flat_table_log}"

TIME=$(date "+%Y-%m-%d %H:%M:%S")
echo "INFO : [Operation: move flat table end.] user: $(whoami), time:${TIME}" | tee -a "${prepare_flat_table_log}"
echo >>"${prepare_flat_table_log}"
