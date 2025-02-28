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


set -o pipefail

function help() {
    echo "Usage: local.sh <COMMAND>"
    echo
    echo "Commands:"
    echo "  init        Install spark and set idea run configurations and install dependencies"
    echo "  up          Download image and start the container(NOTE: current only need zookeeper)."
    echo "  down        Stop the container and remove it"
    echo "  start       Start the container"
    echo "  stop        Stop the container"
    echo "  sample      Load sample data"
    echo "  ps          List containers"
    echo "  interactive Enter the container and get interactive prompts"
    exit 1
}

function info() {
    echo -e "\033[32m$@\033[0m"
}

function warn() {
    echo -e "\033[33m$@\033[0m"
}

function error() {
    echo -e "\033[31m$@\033[0m"
}

PROJECT_DIR=$(cd -P -- "$(dirname -- "$0")/../../.." && pwd -P)
WORKDIR=$(cd -P -- "${PROJECT_DIR}/dev-support/contributor/local" && pwd -P)
FRONTEND_DIR=$(cd -P -- "${PROJECT_DIR}/kystudio" && pwd -P)

KYLIN_HOME="${PROJECT_DIR}"
warn "# KYLIN_HOME=${KYLIN_HOME}"

SPARK_HOME="${PROJECT_DIR}/build/spark"
warn "# SPARK_HOME=${SPARK_HOME}"


function main() {
  if [[ $# == 0 ]]; then
    help
  fi

  case $1 in
    "init" )
        if [[ ! -d "${PROJECT_DIR}/src/server/target" ]]; then
            error "* Please execute 'mvn clean install -DskipTests' first!"
            exit 1
        fi

        if [[ -d "${SPARK_HOME}" ]]; then
            warn "* Spark already exists, will be deleted and re-downloaded"
            rm -rf ${SPARK_HOME}
        fi

        info "* Downloading spark..."
        ${PROJECT_DIR}/build/release/download-spark.sh
        if [[ $? != 0 ]]; then
            warn "  Download spark failed, please manually execute 'download-spark.sh'"
        fi

        info "* Setting IDEA run configurations..."
        if [[ ! -d "${PROJECT_DIR}/.idea/runConfigurations" ]]; then
            mkdir "${PROJECT_DIR}/.idea/runConfigurations"
        fi
        DEFAULT_CONFIGURATION_VERSION=
        if [[ -z ${DEFAULT_CONFIGURATION_VERSION} ]]; then
            DEFAULT_CONFIGURATION_VERSION="community"
        fi
        warn "# IDEA run configuration version is ${DEFAULT_CONFIGURATION_VERSION}..."
        eval "cat <<EOF
            $(<"${WORKDIR}/runConfigurations/BootstrapServer_local_community.xml")
EOF" >"${PROJECT_DIR}/.idea/runConfigurations/BootstrapServer_local_community.xml"

        info "* Build Frontend..."
        cd ${FRONTEND_DIR}
        npm install -g yarn
        yarn install >>/dev/null 2>&1

        info "* Init Done!"
        ;;
    "up" )
        info "* Starting container..."
        docker compose -f "${WORKDIR}/docker-compose.yml" up -d
        if [[ $? != 0 ]]; then
            error "* Start container failed;"
            exit 1
        fi
        info "* Up Done!"
        ;;
    "down" )
        docker compose -f "${WORKDIR}/docker-compose.yml" down
        ;;
    "start" )
        shift
        docker compose -f "${WORKDIR}/docker-compose.yml" start $@
        ;;
    "stop" )
        shift
        docker compose -f "${WORKDIR}/docker-compose.yml" stop $@
        ;;
    "ps" )
        shift
        docker compose -f "${WORKDIR}/docker-compose.yml" ps $@
        ;;
    "interactive" )
        if [[ -z $2 ]]; then
            help
        fi
        docker compose -f "${WORKDIR}/docker-compose.yml" exec $2 bash
        ;;
    *)
        help
        ;;
  esac
}

main $@

