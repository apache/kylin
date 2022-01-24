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
EC2_DEFAULT_USER=ec2-user

### Parameters for Spark and Kylin
#### ${SPARK_VERSION:0:1} get 2 from 2.4.7
GRAFANA_VERSION=8.2.6
HIVE_VERSION=2.3.9
HADOOP_VERSION=3.2.0

### File name
PROMETHEUS_PACKAGE=prometheus-2.31.1.linux-amd64.tar.gz
NODE_EXPORTER_PACKAGE=node_exporter-1.3.1.linux-amd64.tar.gz
HIVE_PACKAGE=apache-hive-${HIVE_VERSION}-bin.tar.gz
HADOOP_PACKAGE=hadoop-${HADOOP_VERSION}.tar.gz

### Parameter for JDK 1.8
JDK_PACKAGE=jdk-8u301-linux-x64.tar.gz
JDK_DECOMPRESS_NAME=jdk1.8.0_301

HOME_DIR=/home/ec2-user

function init_env() {
  HADOOP_DIR=${HOME_DIR}/hadoop
  if [[ ! -d $HADOOP_DIR ]]; then
    mkdir ${HADOOP_DIR}
  fi

  JAVA_HOME=/usr/local/java
  JRE_HOME=${JAVA_HOME}/jre
  OUT_LOG=${HOME_DIR}/shell.stdout
  HADOOP_HOME=${HADOOP_DIR}/hadoop-${HADOOP_VERSION}
  HIVE_HOME=${HADOOP_DIR}/hive

  # extra prometheus env
  PROMETHEUS_HOME=/home/ec2-user/prometheus
  NODE_EXPORTER_HOME=/home/ec2-user/node_exporter

  cat <<EOF >>~/.bash_profile
## Set env variables
### jdk env
export JAVA_HOME=${JAVA_HOME}
export JRE_HOME=${JRE_HOME}
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib

### hadoop env
export HADOOP_HOME=${HADOOP_HOME}

### hive env
export HIVE_HOME=${HIVE_HOME}

### prometheus related env
export PROMETHEUS_HOME=${PROMETHEUS_HOME}
export NODE_EXPORTER_HOME=${NODE_EXPORTER_HOME}

### export all path
export PATH=$HIVE_HOME/bin:$HIVE_HOME/conf:${HADOOP_HOME}/bin:${JAVA_HOME}/bin:$PATH

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
                       --db-host host-for-hive-to-access-rds
                       --db-user user-for-hive-to-access-rds
                       --db-password password-for-hive-to-access-rds
                       --db-port port-for-hive-to-access-rds"
  exit 0
}

if [[ $# -ne 12 ]]; then
  help
fi

while [[ $# != 0 ]]; do
  if [[ $1 == "--bucket-url" ]]; then
    # url same as: /xxx/kylin
    BUCKET_SUFFIX=$2
  elif [[ $1 == "--region" ]]; then
    CURRENT_REGION=$2
  elif [[ $1 == "--db-host" ]]; then
    DATABASE_HOST=$2
  elif [[ $1 == "--db-password" ]]; then
    DATABASE_PASSWORD=$2
  elif [[ $1 == "--db-user" ]]; then
    DATABASE_USER=$2
  elif [[ $1 == "--db-port" ]]; then
    DATABASE_PORT=$2
  else
    help
  fi
  shift
  shift
done

PATH_TO_BUCKET=s3:/${BUCKET_SUFFIX}
CONFIG_PATH_TO_BUCKET=s3a:/${BUCKET_SUFFIX}

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

  logging info "Hadoop prepared ..."
  touch ${HOME_DIR}/.prepared_hadoop
}

function init_hadoop() {
  # match correct region endpoints for hadoop's core-site.xml
  if [[ ${CURRENT_REGION} == "cn-northwest-1" ]]; then
    S3_ENDPOINT=s3.cn-northwest-1.amazonaws.com.cn
  elif [[ ${CURRENT_REGION} == "cn-north-1" ]]; then
    S3_ENDPOINT=s3.cn-north-1.amazonaws.com.cn
  else
    S3_ENDPOINT=s3.${CURRENT_REGION}.amazonaws.com
  fi

  if [[ -f ${HOME_DIR}/.inited_hadoop ]]; then
    logging warn "Hadoop already inited, skip init ..."
  else
    logging info "Init hadoop config ..."
    # replace jars for hadoop, although it don't need to start
    cp hadoop-${HADOOP_VERSION}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar hadoop-${HADOOP_VERSION}/share/hadoop/common/lib/
    cp hadoop-${HADOOP_VERSION}/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar hadoop-${HADOOP_VERSION}/share/hadoop/common/lib/

    cat <<EOF >${HOME_DIR}/hadoop-${HADOOP_VERSION}/etc/hadoop/core-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
	<property>
	    <name>fs.default.name</name>
	    <value>${CONFIG_PATH_TO_BUCKET}/working_dir</value>
	</property>
  <property>
    <name>fs.s3a.endpoint</name>
    <value>${S3_ENDPOINT}</value>
  </property>
</configuration>
EOF

    logging info "Moving hadoop package to ${HADOOP_HOME} ..."
    mv ${HOME_DIR}/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}

    logging warn "touch ${HOME_DIR}/.inited_hadoop ... "
    touch ${HOME_DIR}/.inited_hadoop
  fi

  logging info "Hadoop is ready ..."
}

function prepare_hive() {
  logging info "Preparing hive ..."
  if [[ -f ${HOME_DIR}/.prepared_hive ]]; then
    logging warn "Hive already prepared, enjoy it."
    return
  fi

  if [[ -f ${HOME_DIR}/${HIVE_PACKAGE} ]]; then
    logging warn "${HIVE_PACKAGE} already downloaded, skip download it ..."
  else
    logging info "Downloading ${HIVE_PACKAGE} ..."
    aws s3 cp ${PATH_TO_BUCKET}/tar/${HIVE_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
    #      # wget cost lot time
    #      wget https://downloads.apache.org/hive/hive-${HIVE_VERSION}/${HIVE_PACKAGE}
  fi

  if [[ -d ${HOME_DIR}/apache-hive-${HIVE_VERSION}-bin ]]; then
    logging warn "Hive Package already decompressed, skip decompress ..."
  else
    logging info "Decompress Hive package ..."
    tar -zxf ${HIVE_PACKAGE}
  fi

  if [[ -d ${HIVE_HOME} ]]; then
    logging warn "Hive package already exists, skip moving ..."
  else
    logging info "Moving hive package ${HIVE_PACKAGE%*.*.*} to ${HIVE_HOME} ..."
    mv ${HIVE_PACKAGE%*.*.*} ${HIVE_HOME}
    if [[ $? -ne 0 ]]; then
      logging error " Moving hive package failed, please check ..."
      exit 0
    fi
  fi

  # execute command
  hive --version
  if [[ $? -eq 0 ]]; then
    logging info "Hive ${HIVE_VERSION} is ready ..."
  else
    logging error "Hive not prepared well, please check ..."
    exit 0
  fi

  logging info "Hive prepared successfully ..."
  touch ${HOME_DIR}/.prepared_hive
}

function init_hive() {
  if [[ -f ${HOME_DIR}/.inited_hive ]]; then
    logging warn "Hive already init, skip init ..."
    return
  fi

  cat <<EOF >${HIVE_HOME}/conf/hive-site.xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>${DATABASE_PASSWORD}</value>
    <description>password to use against metastore database</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://${DATABASE_HOST}:${DATABASE_PORT}/hive?createDatabaseIfNotExist=true&amp;useSSL=false</value>
    <description>JDBC connect string for a JDBC metastore</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>com.mysql.jdbc.Driver</value>
    <description>Driver class name for a JDBC metastore</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>${DATABASE_USER}</value>
    <description>Username to use against metastore database;default is root</description>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
    <description>
      Enforce metastore schema version consistency.
      True: Verify that version information stored in metastore matches with one from Hive jars.  Also disable automatic
            schema migration attempt. Users are required to manually migrate schema after Hive upgrade which ensures
            proper metastore schema migration. (Default)
      False: Warn if the version information stored in metastore doesn't match with one from in Hive jars.
    </description>
  </property>
</configuration>
EOF

  # resolve jars conflict
  if [[ ! -d $HIVE_HOME/spark_jar ]]; then
    mkdir -p $HIVE_HOME/spark_jar
    mv $HIVE_HOME/lib/spark-* $HIVE_HOME/spark_jar/
    mv $HIVE_HOME/lib/jackson-module-scala_2.11-2.6.5.jar $HIVE_HOME/spark_jar/
  fi

  if [[ ! -f ${HIVE_HOME}/lib/mysql-connector-java-5.1.40.jar ]]; then
    logging warn "${HIVE_HOME}/lib/mysql-connector-java-5.1.40.jar not exist, download it ..."
    aws s3 cp ${PATH_TO_BUCKET}/jars/mysql-connector-java-5.1.40.jar ${HIVE_HOME}/lib/ --region ${CURRENT_REGION}
  fi

  if [[ ! -f ${HOME_DIR}/.inited_hive_metadata ]]; then
    bash -vx $HIVE_HOME/bin/schematool -dbType mysql -initSchema
    if [[ $? -ne 0 ]]; then
      logging error "Init hive metadata failed, Maybe it was already initialized, please check ..."
    else
      logging info "Hive metadata inited successfully ..."
    fi
    touch ${HOME_DIR}/.inited_hive_metadata
  fi

  if [[ ! -d ${HIVE_HOME}/logs ]]; then
    logging info "Making dir ${HIVE_HOME}/logs"
    mkdir -p $HIVE_HOME/logs
  fi

  logging warn "touch ${HOME_DIR}/.inited_hive ..."
  # make a tag for hive inited
  touch ${HOME_DIR}/.inited_hive
  logging info "Hive is ready ..."
}

function start_hive_metastore() {
  nohup $HIVE_HOME/bin/hive --service metastore >> $HIVE_HOME/logs/hivemetastorelog.log 2>&1 &
  logging info "Hive was logging in $HIVE_HOME/logs, you can check ..."
}

## install mysql db with docker
function start_docker() {
  # check docker whether is running
  docker_status=$(systemctl is-active docker)
  if [[ $docker_status == "active" ]]; then
    logging warn "Docker service is already running, don't need to start it ..."
  else
    logging warn "Docker service is stopped, starting it ..."
    sudo systemctl start docker
    docker_status=$(systemctl is-active docker)
    if [[ $docker_status == "inactive" ]]; then
      logging error "Start docker failed, please check."
      exit 0
    fi
    logging info "Start docker success ..."
  fi
}

function prepare_docker() {
  logging info "Preparing docker ..."

  if [[ -f ${HOME_DIR}/.prepared_docker ]]; then
    logging warn "Docker service already installed, restart it."
    return
  fi

  # check docker whether is installed
  if [[ -x "$(command -v docker)" ]]; then
    docker_version=$(sudo docker -v)
    logging info "Docker is already installed, version is ${docker_version}"
  else
    logging warn "Docker was not installed, now install docker ..."
    sudo yum install docker -y
  fi

  if [[ $(getent group docker) ]]; then
    logging warn "docker group exists, don't need create it."
  else
    logging warn "docker does not exist, create it."
    sudo groupadd docker
  fi

  if id -Gn ${EC2_DEFAULT_USER} | grep docker; then
    logging warn "${EC2_DEFAULT_USER} already in group of docker"
  else
    logging info "Group of docker add user ${EC2_DEFAULT_USER}"
    sudo usermod -aG docker $EC2_DEFAULT_USER
  fi

  touch ${HOME_DIR}/.prepared_docker
  logging info "docker is ready ..."
}

function start_grafana() {
  logging info "Preparing grafana ..."
  if [[ -f ${HOME_DIR}/.prepared_grafana ]]; then
    logging warn "Grafana service already installed, check it."
    return
  fi

  start_docker

  if [[ $(sudo docker ps -q -f name=grafana-${GRAFANA_VERSION}) ]]; then
    logging warn "Grafana-${GRAFANA_VERSION} already running, skip this ..."
  else
    # default user is root !!!
    sudo docker run -d --name grafana-${GRAFANA_VERSION} --restart=always -p 3000:3000 grafana/grafana:${GRAFANA_VERSION}

    if [[ $? -ne 0 ]]; then
      logging error "Grafana start in docker was failed, please check ..."
      exit 0
    fi
  fi
  logging warn "touch ${HOME_DIR}/.prepared_grafana"
  touch ${HOME_DIR}/.prepared_grafana
  logging info "Grafana is ready ..."
}

function prepare_prometheus() {
  logging info "Preparing prometheus ..."
  if [[ -f ${HOME_DIR}/.prepared_prometheus ]]; then
      logging warn "Prometheus already prepared, skip prepare ... "
      return
  fi

  if [[ ! -f ${HOME_DIR}/${PROMETHEUS_PACKAGE} ]]; then
      logging info "Prometheus package ${PROMETHEUS_PACKAGE} not downloaded, downloading it ..."
      aws s3 cp ${PATH_TO_BUCKET}/tar/${PROMETHEUS_PACKAGE} ${HOME_DIR} --region ${CURRENT_REGION}
  else
      logging warn "Prometheus package ${PROMETHEUS_PACKAGE} already download, skip download it."
  fi
  touch ${HOME_DIR}/.prepared_prometheus
  logging info "Prometheus prepared ..."
}

function init_prometheus() {
  logging info "Initializing prometheus ..."
  if [[ -f ${HOME_DIR}/.inited_prometheus ]]; then
      logging warn "Prometheus already inited, skip init ... "
      return
  fi

  if [[ ! -f ${PROMETHEUS_HOME} ]]; then
      logging info "Prometheus home ${PROMETHEUS_HOME} not ready, decompressing ${PROMETHEUS_PACKAGE} ..."
      tar -zxf ${HOME_DIR}/${PROMETHEUS_PACKAGE}
      mv ${HOME_DIR}/${PROMETHEUS_PACKAGE%.tar.gz} ${PROMETHEUS_HOME}
  else
      logging warn "Prometheus home ${PROMETHEUS_PACKAGE} already ready."
  fi

  if [[ ! -d ${PROMETHEUS_HOME}/data ]]; then
    logging info "Prometheus data dir not exists, creating it ..."
    mkdir -p ${PROMETHEUS_HOME}/data
  fi

  touch ${HOME_DIR}/.inited_prometheus
  logging info "Prometheus inited ..."
  # NOTE: prometheus server will start after node_exporter on every node started.
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
  prepare_prometheus
  init_prometheus

  prepare_node_exporter
  init_node_exporter

  prepare_hadoop
  init_hadoop

  prepare_hive
  init_hive

  touch ${HOME_DIR}/.prepared_packages
  logging info "All need packages are ready ..."
}

function start_services_on_other() {
  start_hive_metastore

  # start extra monitor service
  # NOTE: prometheus server will start after all node_exporter on every node started.
  start_node_exporter

  # grafana will start at last
  prepare_docker
  start_grafana
}

function main() {
  prepare_packages
  start_services_on_other
}

main
