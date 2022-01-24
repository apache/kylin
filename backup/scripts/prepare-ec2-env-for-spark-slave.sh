#!/bin/bash

# Note: this script is for AWS ec2 instances
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

function help() {
  logging warn "Invalid input."
  logging warn "Usage: ${BASH_SOURCE[0]}
                    --bucket-url /path/to/bucket
                    --master-host host_ip
                    --worker-number 1 (or 2 or 3 ....)
                    --region region-for-s3
                    --waiting-time time-for-start-services
                    --mode cluster-mode-is-product-or-test
                    --local-soft whether-to-use-local-cache+soft-affinity"
  exit 0
}

if [[ $# -ne 14 ]]; then
  help
fi

while [[ $# != 0 ]]; do
  if [[ $1 == "--bucket-url" ]]; then
    BUCKET_SUFFIX=$2
  elif [[ $1 == "--master-host" ]]; then
    MASTER_HOST=$2
  elif [[ $1 == "--worker-number" ]]; then
    WORKER=$2
  elif [[ $1 == "--region" ]]; then
      CURRENT_REGION=$2
  elif [[ $1 == "--waiting-time" ]]; then
      WAITING_TIME=$2
  elif [[ $1 == "--mode" ]]; then
      WORKER_MODE=$2
  elif [[ $1 == "--local-soft" ]]; then
    LOCAL_CACHE_SOFT_AFFINITY=$2
  else
    help
  fi
  shift
  shift
done

# =============== Env Parameters =================
# Prepare Steps
## Parameter
### Parameters for Spark
#### ${SPARK_VERSION:0:1} get 2 from 2.4.7
HADOOP_VERSION=3.2.0
SPARK_VERSION=3.1.1
KYLIN_VERSION=4.0.0

### Parameter for JDK 1.8
JDK_PACKAGE=jdk-8u301-linux-x64.tar.gz
JDK_DECOMPRESS_NAME=jdk1.8.0_301

LOCAL_CACHE_DIR=/home/ec2-user/ssd

### File name
if [[ $LOCAL_CACHE_SOFT_AFFINITY == "false" ]]; then
  KYLIN_PACKAGE=apache-kylin-${KYLIN_VERSION}-bin-spark${SPARK_VERSION:0:1}.tar.gz
  DECOMPRESSED_KYLIN_PACKAGE=apache-kylin-${KYLIN_VERSION}-bin-spark${SPARK_VERSION:0:1}
else
  KYLIN_PACKAGE=apache-kylin-${KYLIN_VERSION}-bin-spark${SPARK_VERSION:0:1}-soft.tar.gz
  DECOMPRESSED_KYLIN_PACKAGE=apache-kylin-${KYLIN_VERSION}-bin-spark${SPARK_VERSION:0:1}-soft

  # Prepared the local cache dir for local cache + soft affinity

  if [[ ! -d ${LOCAL_CACHE_DIR} ]]; then
      sudo mkdir -p ${LOCAL_CACHE_DIR}
      sudo chmod -R 777 ${LOCAL_CACHE_DIR}
  fi

  if [[ ! -d ${LOCAL_CACHE_DIR}/alluxio-cache-driver ]]; then
      sudo mkdir -p ${LOCAL_CACHE_DIR}/alluxio-cache-driver
      sudo chmod -R 777 ${LOCAL_CACHE_DIR}/alluxio-cache-driver
  fi
fi

SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION:0:3}.tgz
HADOOP_PACKAGE=hadoop-${HADOOP_VERSION}.tar.gz
NODE_EXPORTER_PACKAGE=node_exporter-1.3.1.linux-amd64.tar.gz

HOME_DIR=/home/ec2-user

function init_env() {
    HADOOP_DIR=${HOME_DIR}/hadoop
    if [[ ! -d $HADOOP_DIR ]]; then
      mkdir ${HADOOP_DIR}
    fi

    JAVA_HOME=/usr/local/java
    JRE_HOME=${JAVA_HOME}/jre
    KYLIN_HOME=${HOME_DIR}/${DECOMPRESSED_KYLIN_PACKAGE}
    SPARK_HOME=${HADOOP_DIR}/spark
    OUT_LOG=${HOME_DIR}/shell.stdout
    HADOOP_HOME=${HADOOP_DIR}/hadoop-${HADOOP_VERSION}

    # extra prometheus env
    NODE_EXPORTER_HOME=/home/ec2-user/node_exporter

    cat <<EOF >>~/.bash_profile
## Set env variables
export JAVA_HOME=${JAVA_HOME}
export JRE_HOME=${JRE_HOME}
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
#### hadoop env
export HADOOP_HOME=${HADOOP_HOME}

### prometheus related env
export NODE_EXPORTER_HOME=${NODE_EXPORTER_HOME}

export PATH=${HADOOP_HOME}/bin:${JAVA_HOME}/bin:$PATH

### other
export HOME_DIR=${HOME_DIR}
export KYLIN_HOME=${KYLIN_HOME}
export SPARK_HOME=${SPARK_HOME}
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
  logging info "Scp jdk package from s3: aws s3 cp ${PATH_TO_BUCKET}/tar/${JDK_PACKAGE} ${HOME_DIR} ..."
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
    cp -f ${JAVA_HOME}/jre/lib/security/java.security ${JAVA_HOME}/jre/lib/security/java.security.bak

    ## modify the java.security file
    sed -e "s/\ TLSv1,\ TLSv1.1,//g" -i ${JAVA_HOME}/jre/lib/security/java.security
    logging info "Jdk inited ..."
    touch ${HOME_DIR}/.inited_jdk
}

function prepare_hadoop() {
  if [[ -f ${HOME_DIR}/.prepared_hadoop ]]; then
    logging warn "Hadoop already prepared, skip init ..."
    return
  fi

  if [[ -f ${HOME_DIR}/${HADOOP_PACKAGE} ]]; then
      logging warn "Hadoop package ${HADOOP_PACKAGE} already downloaded, skip download ..."
  else
      logging info "Downloading Hadoop package ${HADOOP_PACKAGE} ..."
      aws s3 cp ${PATH_TO_BUCKET}/tar/${HADOOP_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
#      # wget cost lot time
#      wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE}
  fi

  if [[ -d ${HOME_DIR}/hadoop-${HADOOP_VERSION} ]]; then
      logging warn "Hadoop Package decompressed, skip decompress ..."
  else
      logging info "Decompress Hadoop package ..."
      tar -zxf ${HADOOP_PACKAGE}
  fi

  logging info "Moving hadoop package to ${HADOOP_HOME} ..."
  mv ${HOME_DIR}/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}

  logging info "Hadoop prepared ..."
  touch ${HOME_DIR}/.prepared_hadoop
}

function prepare_spark() {
  if [[ -f ${HOME_DIR}/.prepared_spark ]]; then
    logging warn "Spark already prepared, enjoy it."
    return
  fi

  logging info "Downloading Spark-${SPARK_VERSION} ..."
  ## download spark
  if [[ -f ${HOME_DIR}/${SPARK_PACKAGE} ]]; then
      logging warn "${SPARK_PACKAGE} already download, skip download it."
  else
      logging warn "Downloading ${SPARK_PACKAGE} ..."
      aws s3 cp ${PATH_TO_BUCKET}/tar/${SPARK_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
#      # wget cost lot time
#      wget http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}
  fi

  if [[ -d  ${HOME_DIR}/${SPARK_PACKAGE%*.*} ]]; then
      logging warn "Spark package already decompressed, skip decompress ..."
  else
      logging info "Decompressing ${SPARK_PACKAGE} ..."
      ### unzip spark tar file
      tar -zxf ${SPARK_PACKAGE}
  fi

  if [[ -d ${SPARK_HOME} ]]; then
    logging warn "${SPARK_HOME} already exists, skip moving ..."
  else
    logging info "Moving ${SPARK_PACKAGE%*.*} to ${SPARK_HOME}"
    mv ${SPARK_PACKAGE%*.*} ${SPARK_HOME}
  fi

  logging info "Prepare Spark-${SPARK_VERSION} success."
  touch ${HOME_DIR}/.prepared_spark
}

function init_spark() {
  if [[ -f ${HOME_DIR}/.inited_spark ]]; then
    logging warn "Spark already inited, enjoy it."
    return
  fi

  # copy needed jars
  if [[ ! -f $SPARK_HOME/jars/hadoop-aws-${HADOOP_VERSION}.jar ]]; then
    cp $HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar $SPARK_HOME/jars/
    if [[ $? -ne 0 ]]; then
        logging error "Copy $HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar $SPARK_HOME/jars/ failed ..."
        exit 0
    fi
  fi

  if [[ ! -f $SPARK_HOME/jars/aws-java-sdk-bundle-1.11.375.jar ]]; then
    cp $HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar $SPARK_HOME/jars/
    if [[ $? -ne 0 ]]; then
        logging error "Copy $HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar $SPARK_HOME/jars/ failed ..."
        exit 0
    fi
  fi

  #Support local cache + soft affinity
  if [[ $LOCAL_CACHE_SOFT_AFFINITY == "true" ]]; then
    if [[ ! -f $SPARK_HOME/jars/kylin-soft-affinity-cache-4.0.0-SNAPSHOT.jar ]]; then
      logging info "Downloading kylin-soft-affinity-cache-4.0.0-SNAPSHOT.jar to $SPARK_HOME/jars/ ..."
      aws s3 cp ${PATH_TO_BUCKET}/jars/kylin-soft-affinity-cache-4.0.0-SNAPSHOT.jar $SPARK_HOME/jars/ --region ${CURRENT_REGION}
    fi

    if [[ ! -f $SPARK_HOME/jars/alluxio-2.6.1-client.jar ]]; then
      logging info "Downloading alluxio-2.6.1-client.jar to $SPARK_HOME/jars/ ..."
      aws s3 cp ${PATH_TO_BUCKET}/jars/alluxio-2.6.1-client.jar $SPARK_HOME/jars/ --region ${CURRENT_REGION}
    fi
  fi

  # Support prometheus metrics
  cat <<EOF > ${SPARK_HOME}/conf/metrics.properties
*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
*.sink.prometheusServlet.path=/metrics/prometheus
master.sink.prometheusServlet.path=/metrics/master/prometheus
applications.sink.prometheusServlet.path=/metrics/applications/prometheus
EOF

  logging info "Spark inited ..."
  touch ${HOME_DIR}/.inited_spark
  logging info "Spark is ready ..."
}

function start_spark_worker() {
  # TODO: fix hard code for waiting time
  sleep ${WAITING_TIME}
  if [[ $WORKER_MODE == 'product' ]]; then
    # product:
    # # ec2 instance type is m5.4xlarge which has 16 cores! Set 15 to Spark master.
    # # Also set 60 GB memory for cluster
    $SPARK_HOME/sbin/start-worker.sh ${MASTER_HOST}:7077 -c 15 -m 63G
  else
    # test: ec2 instance type is m5.2xlarget which has 8cores!
    $SPARK_HOME/sbin/start-worker.sh ${MASTER_HOST}:7077
  fi
  if [[ $? -ne 0 ]]; then
      logging error "spark start worker ${WORKER} failed, please check ..."
      exit 0
  fi
  logging info "Start Spark worker ${WORKER} successfully ..."
}

function prepare_kylin() {
  logging info "Preparing kylin ..."

  if [[ -f ${HOME_DIR}/.prepared_kylin ]]; then
      logging warn "Kylin already prepared ..."
      return
  fi

  if [[ -f ${HOME_DIR}/${KYLIN_PACKAGE} ]]; then
      logging warn "Kylin package already downloaded, skip download it ..."
  else
      logging info "Kylin-${KYLIN_VERSION} downloading ..."
      aws s3 cp ${PATH_TO_BUCKET}/tar/${KYLIN_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
#      # wget cost lot time
#      wget https://archive.apache.org/dist/kylin/apache-kylin-${KYLIN_VERSION}/${KYLIN_PACKAGE}
  fi

  if [[ -d ${HOME_DIR}/${DECOMPRESSED_KYLIN_PACKAGE} ]]; then
      logging warn "Kylin package already decompress, skip decompress ..."
  else
      logging warn "Kylin package decompressing ..."
      ### unzip kylin tar file
      tar -zxf ${KYLIN_PACKAGE}
  fi

  logging info "Kylin inited ..."
  touch ${HOME_DIR}/.prepared_kylin
  logging info "Kylin prepared ..."
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
  # start node_exporter quickly
  start_node_exporter

  prepare_hadoop

  prepare_spark
  init_spark

  prepare_kylin

  touch ${HOME_DIR}/.prepared_packages
  logging info "All need packages are ready ..."
}

function start_services_on_slave() {
  start_spark_worker
}

function main() {
  prepare_packages
  start_services_on_slave
}

main
