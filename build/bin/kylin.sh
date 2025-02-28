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
  echo "Usage: kylin.sh <COMMAND>"
  echo
  echo "Commands:"
  echo "  -D                    -D[ skipCheck ]"
  echo "                        skip some check when bootstrap, eg. bash kylin.sh -DskipCheck start"
  echo "                        -D[ withoutGluten ]"
  echo "                        skip skip load gluten dependencies, eg. bash kylin.sh -DwithoutGluten start"
  echo "  -C                    -C[ true | false ], default true, use the local properties or not"
  echo "  start                 start kylin"
  echo "  restart               restart kylin"
  echo "  stop                  stop kylin"
  echo "  org.apache.kylin.*    run tool"
  echo "  interactive Enter for bootstrap"
}

function parseArgs() {
  case $1 in
  "skipCheck")
    export KYLIN_SKIP_CHECK=1
    ;;
  "withoutGluten")
    export KYLIN_WITH_GLUTEN=0
    ;;
  *)
    echo "Invalid option -D: -$1" >&2
    exit 1
    ;;
  esac
}

function checkArguments() {
   local local_properties="false"
   # enabled when check env by pass
   if [[ -f ${KYLIN_HOME}/bin/check-env-bypass ]]; then
     local_properties="true"
   fi

  case $1 in
  "start" | "restart")
    export _KYLIN_GET_PROPERTIES_FROM_LOCAL_CACHE=${_KYLIN_GET_PROPERTIES_FROM_LOCAL_CACHE:-"${local_properties}"}
    exportPropertiesToFile
    "${KYLIN_HOME}"/sbin/rotate-logs.sh "$@"
    export KYLIN_SKIP_ROTATE_LOG=1
    ;;
  "stop")
    export _KYLIN_GET_PROPERTIES_FROM_LOCAL_CACHE=${_KYLIN_GET_PROPERTIES_FROM_LOCAL_CACHE:-"${local_properties}"}
    ;;
  "spawn")
    export _KYLIN_GET_PROPERTIES_FROM_LOCAL_CACHE=${_KYLIN_GET_PROPERTIES_FROM_LOCAL_CACHE:-"${local_properties}"}
    "${KYLIN_HOME}"/sbin/rotate-logs.sh "$@"
    export KYLIN_SKIP_ROTATE_LOG=1
    ;;
  *) ;;
  esac
}

function main() {
  # parsed arguments
  while getopts "vD:C:h" opt; do
    case ${opt} in
    v)
      export verbose=true
      ;;
    D)
      parseArgs "$OPTARG" || exit 1
      ;;
    C)
      export _KYLIN_GET_PROPERTIES_FROM_LOCAL_CACHE="$OPTARG"
      ;;
    h)
      help
      exit 0
      ;;
    *)
      echo "Invalid option: -$OPTARG" && exit 1
      ;;
    esac
  done
  shift $((OPTIND - 1))

  # init
  source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh "$@"
  mkdir -p "${KYLIN_HOME}"/logs
  ERR_LOG=${KYLIN_HOME}/logs/shell.stderr
  OUT_LOG=${KYLIN_HOME}/logs/shell.stdout

  # check action arguments
  checkArguments "$@"

  echo "-----------------------  log start  -----------------------" >>${ERR_LOG}
  echo "-----------------------  log start  -----------------------" >>${OUT_LOG}
  bash -x "${KYLIN_HOME}"/sbin/bootstrap.sh "$@" 2>>"${ERR_LOG}" | tee -a ${OUT_LOG}
  ret=${PIPESTATUS[0]}
  echo "-----------------------  log end  -------------------------" >>${ERR_LOG}
  echo "-----------------------  log end  -------------------------" >>${OUT_LOG}
  exit ${ret}
}

main "$@"
