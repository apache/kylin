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
    echo "Usage: sandbox.sh <COMMAND>"
    echo
    echo "Commands:"
    echo "  init        Install spark and set idea run configurations"
    echo "  up          Download image and start the container"
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

PROJECT_DIR=$(cd -P -- "$(dirname -- "$0")/../.." && pwd -P)
WORKDIR=$(cd -P -- "${PROJECT_DIR}/dev-support/sandbox" && pwd -P)

KYLIN_HOME="${PROJECT_DIR}"
warn "# KYLIN_HOME=${KYLIN_HOME}"

KYLIN_CONF="${WORKDIR}/conf"
warn "# KYLIN_CONF=${KYLIN_CONF}"

SPARK_HOME="${PROJECT_DIR}/build/spark"
warn "# SPARK_HOME=${SPARK_HOME}"

HADOOP_CONF_DIR="${WORKDIR}/conf"
warn "# HADOOP_CONF_DIR=${HADOOP_CONF_DIR}\n"

function main() {
  if [[ $# == 0 ]]; then
    help
  fi

  export HADOOP_CORE_SITE=${WORKDIR}/conf/hadoop/core-site.xml

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

        info "* Setting spark dependency..."
#        cp ${PROJECT_DIR}/src/server/target/jars/log4j* ${SPARK_HOME}/jars
#        cp ${WORKDIR}/libs/mysql-connector-java-8.0.16.jar ${SPARK_HOME}/jars

        info "* Setting IDEA run configurations..."
        if [[ ! -d "${PROJECT_DIR}/.idea/runConfigurations" ]]; then
            mkdir "${PROJECT_DIR}/.idea/runConfigurations"
        fi

        eval "cat <<EOF
            $(<"${WORKDIR}/runConfigurations/BootstrapServer_docker_sandbox_.xml")
EOF" >"${PROJECT_DIR}/.idea/runConfigurations/BootstrapServer_docker_sandbox_.xml"

        info "* Init Done!"
        ;;

    "up" )
        info "* Starting container..."
        docker compose -f "${WORKDIR}/docker-compose.yml" up -d
        if [[ $? != 0 ]]; then
            error "* Start container failed;"
            exit 1
        fi

        info "* Initing metadata..."
        sleep 10
        local retry=0
        while true; do
            docker compose -f "${WORKDIR}/docker-compose.yml" exec mysql bash -c "mysql -uroot -proot <<EOF
                CREATE DATABASE IF NOT EXISTS kylin;
EOF"
            if [[ $? == 0 ]]; then
                break;
            fi

            (( retry++ ))
            if [[ $retry -gt 5 ]]; then
                warn "* Init metadata failed, please manually enter the MySQL container to create database 'kylin'"
                exit 1
            fi

            warn "  retry connect mysql container again($retry/5)..."
            sleep 5
        done

        docker compose -f "${WORKDIR}/docker-compose.yml" exec datanode hadoop dfs -mkdir -p '/kylin/spark-history'
        if [[ $? != 0 ]]; then
            warn "Please manually enter the Datanode container to create hdfs folder '/kylin/spark-history'"
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
    "sample" )
        info "* Loading SSB data to HDFS..."
        docker compose -f "${WORKDIR}/docker-compose.yml" cp ${PROJECT_DIR}/src/examples/sample_cube/data datanode:/tmp/ssb
        docker compose -f "${WORKDIR}/docker-compose.yml" exec datanode bash -c "hdfs dfs -mkdir -p /tmp/sample_cube/data \
            && hdfs dfs -put /tmp/ssb/* /tmp/sample_cube/data/"

        info "* Loading SSB data to Hive..."
        docker compose -f "${WORKDIR}/docker-compose.yml" cp ${PROJECT_DIR}/src/examples/sample_cube/create_sample_ssb_tables.sql hiveserver:/tmp/
        docker compose -f "${WORKDIR}/docker-compose.yml" exec hiveserver bash -c "hive -e 'CREATE DATABASE IF NOT EXISTS SSB' \
            && hive --hivevar hdfs_tmp_dir=/tmp --database SSB -f /tmp/create_sample_ssb_tables.sql"

#        info "* Loading TPC-DS data to HDFS..."
#        docker compose -f "${WORKDIR}/docker-compose.yml" cp ${PROJECT_DIR}/src/examples/sample_cube/tpcds/data datanode:/tmp/tpcds
#        docker compose -f "${WORKDIR}/docker-compose.yml" exec datanode bash -c "hdfs dfs -mkdir -p /tmp/sample_cube/tpcds \
#            && hdfs dfs -put /tmp/tpcds/* /tmp/sample_cube/tpcds/"
#
#        info "* Loading TPC-DS data to Hive..."
#        docker compose -f "${WORKDIR}/docker-compose.yml" cp ${PROJECT_DIR}/src/examples/sample_cube/tpcds/create_sample_tpcds_tables.sql hiveserver:/tmp/
#        docker compose -f "${WORKDIR}/docker-compose.yml" exec hiveserver bash -c "hive -e 'CREATE DATABASE IF NOT EXISTS TPCDS' \
#            && hive --hivevar hdfs_tmp_dir=/tmp --database TPCDS -f /tmp/create_sample_tpcds_tables.sql"

#        info "* Loading streaming data to Kafka"
#        docker compose -f "${WORKDIR}/docker-compose.yml" cp ${WORKDIR}/streaming_data kafka:/tmp/streaming_data
#        docker compose -f "${WORKDIR}/docker-compose.yml" exec kafka bash -c "/opt/bitnami/kafka/bin/kafka-console-producer.sh \
#            --broker-list kafka:9092 --topic streaming_ssb_lineorder < /tmp/streaming_data/STREAMING_SSB_LINEORDER.csv >/dev/null"
        ;;
    *)
        help
        ;;
  esac
}

main $@

