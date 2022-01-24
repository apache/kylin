#!/bin/bash

# Note: this script is for Creating Zookeeper and DB to hold cluster
# This Script contains services which are Zookeeper, JDK and Mysql DB
set -e

# ============= Utils function ============
function info() {
  # shellcheck disable=SC2145
  echo -e "\033[32m$@\033[0m"
}

function warn() {
  # shellcheck disable=SC2145
  echo -e "\033[33m$@\033[0m"
}

function error() {
  # shellcheck disable=SC2145
  echo -e "\033[31m$@\033[0m"
}

function logging() {
  case $1 in
  "info")
    shift
    # shellcheck disable=SC2068
    info $@
    ;;
  "warn")
    shift
    # shellcheck disable=SC2068
    warn $@
    ;;
  "error")
    shift
    # shellcheck disable=SC2068
    error $@
    ;;
  *)
    # shellcheck disable=SC2068
    echo -e $@
    ;;
  esac
}

set +e

# =============== Env Parameters =================
# Prepare Steps
## Parameter
### Parameters for Spark and Kylin
#### ${SPARK_VERSION:0:1} get 2 from 2.4.7
ZOOKEEPER_VERSION=3.4.13

### File name
ZOOKEEPER_PACKAGE=zookeeper-${ZOOKEEPER_VERSION}.tar.gz
PROMETHEUS_PACKAGE=prometheus-2.31.1.linux-amd64.tar.gz
NODE_EXPORTER_PACKAGE=node_exporter-1.3.1.linux-amd64.tar.gz

### Parameter for JDK 1.8
JDK_PACKAGE=jdk-8u301-linux-x64.tar.gz
JDK_DECOMPRESS_NAME=jdk1.8.0_301

HOME_DIR=/home/ec2-user

CURRENT_HOST=$(hostname -i)

function init_env() {
  HADOOP_DIR=${HOME_DIR}/hadoop
  if [[ ! -d $HADOOP_DIR ]]; then
    mkdir ${HADOOP_DIR}
  fi

  JAVA_HOME=/usr/local/java
  JRE_HOME=${JAVA_HOME}/jre
  ZOOKEEPER_HOME=${HADOOP_DIR}/zookeeper
  OUT_LOG=${HOME_DIR}/shell.stdout

  # extra prometheus env
  PROMETHEUS_HOME=/home/ec2-user/prometheus
  NODE_EXPORTER_HOME=/home/ec2-user/node_exporter

  cat <<EOF >>~/.bash_profile
## Set env variables
### jdk env
export JAVA_HOME=${JAVA_HOME}
export JRE_HOME=${JRE_HOME}
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib

### zookeeper env
export ZOOKEEPER_HOME=${ZOOKEEPER_HOME}

### prometheus related env
export PROMETHEUS_HOME=${PROMETHEUS_HOME}
export NODE_EXPORTER_HOME=${NODE_EXPORTER_HOME}

### export all path
export PATH=${JAVA_HOME}/bin:${ZOOKEEPER_HOME}/bin:$PATH

### other
export HOME_DIR=${HOME_DIR}
export OUT_LOG=$OUT_LOG
EOF
}

if [[ ! -f ~/.inited_env ]]; then
  logging info "Env variables not init, init it first ..."
  init_env
  touch ~/.inited_env
else
  logging warn "Env variables already inited, source it ..."
fi
source ~/.bash_profile
exec 2>>${OUT_LOG}
set -o pipefail
# ================ Main Functions ======================
function help() {
  logging warn "Invalid input."
  logging warn "Usage: ${BASH_SOURCE[0]}
                       --bucket-url /path/to/bucket/without/prefix
                       --region region-for-current-instance
                       --zk-num current-zookeeper-number"
  exit 0
}

if [[ $# -ne 6 ]]; then
  help
fi

while [[ $# != 0 ]]; do
  if [[ $1 == "--bucket-url" ]]; then
    # url same as: /xxx/kylin
    BUCKET_SUFFIX=$2
  elif [[ $1 == "--region" ]]; then
    CURRENT_REGION=$2
  elif [[ $1 == "--zk-num" ]]; then
    ZK_NUM=$2
  else
    help
  fi
  shift
  shift
done

PATH_TO_BUCKET=s3:/${BUCKET_SUFFIX}

# Main Functions and Steps
## prepare jdk env
function prepare_jdk() {
  logging info "Preparing Jdk ..."
  if [[ -f ${HOME_DIR}/.prepared_jdk ]]; then
    logging warn "Jdk already prepared, enjoy it."
    return
  fi

  # copy jdk from s3 bucket to ec2 instances, so user need to upload jdk package first
  aws s3 cp ${PATH_TO_BUCKET}/tar/${JDK_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
  # unzip jdk: tar -C /extract/to/path -xzvf /path/to/archive.tar.gz
  tar -zxf ${JDK_PACKAGE}
  sudo mv ${JDK_DECOMPRESS_NAME} ${JAVA_HOME}
  if [[ $? -ne 0 ]]; then
    logging error "Java package was not installed well, pleas check ..."
    exit 0
  fi
  logging info "Jdk inited ..."
  touch ${HOME_DIR}/.prepared_jdk
  logging info "Jdk is ready ..."
}

function init_jdk() {
  if [[ -f ${HOME_DIR}/.inited_jdk ]]; then
    logging warn "Jdk already inited, skip init ..."
    return
  fi
  # this function is remove the unsupport tls rules in java which version of 1.8.291 and above
  ## backup
  cp -f $JAVA_HOME/jre/lib/security/java.security $JAVA_HOME/jre/lib/security/java.security.bak

  ## modify the java.security file
  sed -e "s/\ TLSv1,\ TLSv1.1,//g" -i $JAVA_HOME/jre/lib/security/java.security
  logging info "Jdk inited ..."
  touch ${HOME_DIR}/.inited_jdk
}

function prepare_zookeeper() {
  logging info "Preparing zookeeper ..."
  if [[ -f ${HOME_DIR}/.prepared_zookeeper ]]; then
    logging warn "Zookeeper service already installed, restart it."
    return
  fi

  if [[ -f ./${ZOOKEEPER_PACKAGE} ]]; then
    logging warn "Zookeeper package already download, skip download it"
  else
    logging info "Downloading Zookeeper package ${ZOOKEEPER_PACKAGE} ..."
    aws s3 cp ${PATH_TO_BUCKET}/tar/${ZOOKEEPER_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
    #      # wget cost lot time
    #      wget http://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/${ZOOKEEPER_PACKAGE}
  fi

  if [[ -d ${HOME_DIR}/zookeeper-${ZOOKEEPER_VERSION} ]]; then
    logging warn "Zookeeper Package decompressed, skip decompress ..."
  else
    logging info "Decompress Zookeeper package ..."
    tar -zxf ${ZOOKEEPER_PACKAGE}
  fi

  logging info "Zookeeper prepared ..."
  touch ${HOME_DIR}/.prepared_zookeeper
}

function init_zookeeper() {
  if [[ -f ${HOME_DIR}/.inited_zookeeper ]]; then
    logging warn "Zookeeper already inited ..."
  else
    logging info "Init Zookeeper config ..."
    # copy cfg to set fake zk cluster
    cp zookeeper-${ZOOKEEPER_VERSION}/conf/zoo_sample.cfg zookeeper-${ZOOKEEPER_VERSION}/conf/zoo.cfg

    cat <<EOF >zookeeper-${ZOOKEEPER_VERSION}/conf/zoo.cfg
# zoo${ZK_NUM}.cfg
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/tmp/zookeeper/zk${ZK_NUM}/data
dataLogDir=/tmp/zookeeper/zk${ZK_NUM}/log
clientPort=2181
# server.${ZK_NUM}=${CURRENT_HOST}:2287:3387
EOF
    mkdir -p /tmp/zookeeper/zk${ZK_NUM}/log
    mkdir -p /tmp/zookeeper/zk${ZK_NUM}/data
    echo ${ZK_NUM} >>/tmp/zookeeper/zk${ZK_NUM}/data/myid

    logging info "Moving ${HOME_DIR}/zookeeper-${ZOOKEEPER_VERSION} to ${ZOOKEEPER_HOME} ..."
    mv ${HOME_DIR}/zookeeper-${ZOOKEEPER_VERSION} ${ZOOKEEPER_HOME}

    logging warn "touch ${HOME_DIR}/.inited_zookeeper ..."
    touch ${HOME_DIR}/.inited_zookeeper
  fi
  logging info "Zookeeper is ready ..."
}

function prepare_node_exporter() {
  logging info "Preparing node_exporter ..."
  if [[ -f ${HOME_DIR}/.prepared_node_exporter ]]; then
      logging warn "NODE_EXPORTER already prepared, skip prepare ... "
      return
  fi

  if [[ ! -f ${HOME_DIR}/${NODE_EXPORTER_PACKAGE} ]]; then
      logging info "NODE_EXPORTER package ${NODE_EXPORTER_PACKAGE} not downloaded, downloading it ..."
      aws s3 cp ${PATH_TO_BUCKET}/tar/${NODE_EXPORTER_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
  else
      logging warn "NODE_EXPORTER package ${NODE_EXPORTER_PACKAGE} already download, skip download it."
  fi
  touch ${HOME_DIR}/.prepared_node_exporter
  logging info "NODE_EXPORTER prepared ..."
}

function init_node_exporter() {
  logging info "Initializing node_exporter ..."
  if [[ -f ${HOME_DIR}/.inited_node_exporter ]]; then
      logging warn "NODE_EXPORTER already inited, skip init ... "
      return
  fi

  if [[ ! -f ${NODE_EXPORTER_HOME} ]]; then
      logging info "NODE_EXPORTER home ${NODE_EXPORTER_HOME} not ready, decompressing ${NODE_EXPORTER_PACKAGE} ..."
      tar -zxf ${HOME_DIR}/${NODE_EXPORTER_PACKAGE}
      mv ${HOME_DIR}/${NODE_EXPORTER_PACKAGE%.tar.gz} ${NODE_EXPORTER_HOME}
  else
      logging warn "NODE_EXPORTER home ${PROMETHEUS_PACKAGE} already ready."
  fi
  touch ${HOME_DIR}/.inited_node_exporter
  logging info "NODE_EXPORTER inited ..."
}

function start_node_exporter() {
    # NOTE: default node_exporter port 9100
    logging info "Start node_exporter ..."
    nohup ${NODE_EXPORTER_HOME}/node_exporter >> ${NODE_EXPORTER_HOME}/node.log 2>&1 &
}

function prepare_packages() {
  if [[ -f ${HOME_DIR}/.prepared_packages ]]; then
    logging warn "Packages already prepared, skip prepare ..."
    return
  fi

  prepare_jdk
  init_jdk

  # add extra monitor service
  prepare_node_exporter
  init_node_exporter

  prepare_zookeeper
  init_zookeeper

  touch ${HOME_DIR}/.prepared_packages
  logging info "All need packages are ready ..."
}

function start_services_on_other() {
  # Note: start zookeeper after created all needed nodes, because need to update every zk cfg.

  # start extra monitor service
  # NOTE: prometheus server will start after all node_exporter on every node started.
  start_node_exporter

}

function main() {
  prepare_packages
  start_services_on_other
}

main
