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

if [[ -z $KYLIN_HOME ]]; then
  export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

# tmp config file
_KYLIN_CACHED_CONFIG_FILE=${_KYLIN_CACHED_CONFIG_FILE:-"${KYLIN_HOME}/conf/._kylin_properties_"}
# read the configuration from the local cache (true | false)
GET_PROPERTIES_FROM_LOCAL_CACHE=${_KYLIN_GET_PROPERTIES_FROM_LOCAL_CACHE}

function verboseLog() {
  echo $(date '+%F %H:%M:%S') "$@" >>"${KYLIN_HOME}"/logs/shell.stderr
}

function quit() {
  verboseLog "$@"
  exit 1
}

function help() {
  echo "Usage: get-properties <COMMAND>"
  echo
  echo "Commands:"
  echo "  -c        get properties from cache(local disk)"
  echo "  -r        get properties in real time, this options support backwards config"
  echo "  -e        [file_path]only export properties to cache(local disk)"
  echo "  -h        print help"
  echo "  interactive Enter to get kylin properties"
}

function runTool() {

  if [[ -z ${MAPR_HOME} ]]; then
    export MAPR_HOME="/opt/mapr"
  fi

  if [[ -z ${kylin_hadoop_conf_dir} ]]; then
    export kylin_hadoop_conf_dir=$KYLIN_HOME/hadoop_conf
  fi

  local KYLIN_KERBEROS_OPTS=""
  if [[ -f ${KYLIN_HOME}/conf/krb5.conf ]]; then
    KYLIN_KERBEROS_OPTS="-Djava.security.krb5.conf=${KYLIN_HOME}/conf/krb5.conf"
  fi

  local SPARK_HOME=${KYLIN_HOME}/spark

  local kylin_tools_log4j=""
  if [[ -f ${KYLIN_HOME}/conf/kylin-tools-log4j.xml ]]; then
    kylin_tools_log4j="file:${KYLIN_HOME}/conf/kylin-tools-log4j.xml"
  else
    kylin_tools_log4j="file:${KYLIN_HOME}/tool/conf/kylin-tools-log4j.xml"
  fi

  mkdir -p "${KYLIN_HOME}"/logs
  local result=$(java ${KYLIN_KERBEROS_OPTS} -Dlog4j.configurationFile=${kylin_tools_log4j} -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} -Dhdp.version=current -cp "${kylin_hadoop_conf_dir}:${KYLIN_HOME}/lib/ext/*:${KYLIN_HOME}/server/jars/*:${SPARK_HOME}/jars/*" "$@" 2>>${KYLIN_HOME}/logs/shell.stderr)
  echo "$result"
}

# get properties from cache(local disk)
function getPropertiesFromLocal() {
  if [[ $# != 1 ]]; then
    echo 'Invalid input from local'
    exit 1
  fi

  if [[ "${GET_PROPERTIES_FROM_LOCAL_CACHE}" == "true" && -e ${_KYLIN_CACHED_CONFIG_FILE} ]]; then
    local inputP=$1
    if [[ ${inputP} = kap.* ]]; then
      quit "local properties doesn't support kap.*"
    elif [[ ${inputP} = *. ]]; then
      grep -F "${inputP}" "${_KYLIN_CACHED_CONFIG_FILE}" | sed -e "s/${inputP}//g"
    else
      grep -F "${inputP}=" "${_KYLIN_CACHED_CONFIG_FILE}" | cut -d "=" -f 2-
      verboseLog "getProperties ${inputP} from local success"
    fi
  else
    if [[ "${GET_PROPERTIES_FROM_LOCAL_CACHE}" != true ]]; then
      verboseLog "Please turn _KYLIN_GET_PROPERTIES_FROM_LOCAL_CACHE:${GET_PROPERTIES_FROM_LOCAL_CACHE} to true."
    else
      verboseLog "Cannot find local cache file:${_KYLIN_CACHED_CONFIG_FILE}!"
    fi
    return 1
  fi
}

# get properties in real time
function getPropertiesFromKylin() {
  if [ $# != 1 ]; then
    if [[ $# -lt 2 || $2 != 'DEC' ]]; then
      echo 'Invalid input'
      exit 1
    fi
  fi
  runTool org.apache.kylin.tool.KylinConfigCLI "$@"
}

# 1. export properties to tmp file ${_KYLIN_CACHED_CONFIG_FILE}.tmp
# 2. rename .tmp file
function exportPropertiesToLocal() {
  if [[ "${GET_PROPERTIES_FROM_LOCAL_CACHE}" != "true" ]]; then
    quit "cannot export properties to local when _KYLIN_GET_PROPERTIES_FROM_LOCAL_CACHE=false"
  fi

  local export_file=${1:-${_KYLIN_CACHED_CONFIG_FILE}}
  local tmp_config_file="${export_file}.tmp"

  if [[ -e ${tmp_config_file} ]]; then
    verboseLog "tmp file already exist, try to remove it. path:${tmp_config_file}"
    rm -f "${tmp_config_file}"
  fi

  # export config file
  local result=$(runTool org.apache.kylin.tool.KylinConfigExporterCLI "${tmp_config_file}")
  verboseLog "${result:-"export success"}"

  if [[ $? -ne 0 || ! -e ${tmp_config_file} ]]; then
    quit "export properties failed"
  else
    # rename
    mv "${tmp_config_file}" "${export_file}"
    if [[ $? -ne 0 || ! -e ${export_file} ]]; then
      quit "mv properties failed"
    fi
  fi
}

function getProperties() {
  if [[ "${GET_PROPERTIES_FROM_LOCAL_CACHE}" == "true" ]]; then
    getPropertiesFromLocal "$@"
    if [[ $? -ne 0 ]]; then
      verboseLog "get from cache failed, try to get in real time again" "$@"
      GET_PROPERTIES_FROM_LOCAL_CACHE=""
      getPropertiesFromKylin "$@"
    fi
  else
    getPropertiesFromKylin "$@"
  fi
}

function main() {
  if [[ $# == 0 ]]; then
    help
    exit 0
  fi

  while getopts "c:r:eh" opt; do
    case ${opt} in
    c)
      shift
      GET_PROPERTIES_FROM_LOCAL_CACHE="true"
      getPropertiesFromLocal "$@" || echo ""
      exit $?
      ;;
    r)
      shift
      GET_PROPERTIES_FROM_LOCAL_CACHE=""
      getProperties "$@"
      exit $?
      ;;
    e)
      shift
      exportPropertiesToLocal "$@"
      exit $?
      ;;
    h)
      help
      exit $?
      ;;
    *)
      verboseLog "Invalid option: -$OPTARG"
      exit 1
      ;;
    esac
  done
  getProperties "$@"
}

main "$@"
