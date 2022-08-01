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

# source me

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh

KYLIN_ENV_CHANNEL=`$KYLIN_HOME/bin/get-properties.sh kylin.env.channel`

KERBEROS_ENABLED=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.enabled`
KYLIN_KRB5CONF=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.krb5-conf`

function fetchCloudHadoopConf() {
    mkdir -p ${KYLIN_HOME}/hadoop_conf
    CLOUD_HADOOP_CONF_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.cloud.hadoop-conf-dir`
    checkAndCopyFile $CLOUD_HADOOP_CONF_DIR/core-site.xml
    checkAndCopyFile $CLOUD_HADOOP_CONF_DIR/hive-site.xml
    checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hiveserver2-site.xml
    checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hivemetastore-site.xml
}

function fetchKylinHadoopConf() {

    export FI_ENV_PLATFORM=

    ## FusionInsight platform C60.
    if [ -n "$BIGDATA_HOME" ]
    then
        FI_ENV_PLATFORM=$BIGDATA_HOME
    fi

    ## FusionInsight platform C70/C90.
    if [ -n "$BIGDATA_CLIENT_HOME" ]
    then
        FI_ENV_PLATFORM=$BIGDATA_CLIENT_HOME
    fi

    if [[ -d ${kylin_hadoop_conf_dir} ]]; then
        return
    fi

    if [ -n "$FI_ENV_PLATFORM" ]
    then
        mkdir -p ${KYLIN_HOME}/hadoop_conf
        # FI platform
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/core-site.xml
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/hdfs-site.xml
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/yarn-site.xml
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/mapred-site.xml
        checkAndCopyFIHiveSite
        checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hiveserver2-site.xml
        checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hivemetastore-site.xml

        # don't find topology.map in FI
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/topology.py
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/ssl-client.xml
        checkAndCopyFile $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/hadoop-env.sh
    elif [ -n "$TDH_CLIENT_HOME" ]
    then
        mkdir -p ${KYLIN_HOME}/hadoop_conf

        echo "Start copy tdh client home: $TDH_CLIENT_HOME"
        checkAndCopyFile $TDH_CLIENT_HOME/conf/hadoop/core-site.xml
        checkAndCopyFile $TDH_CLIENT_HOME/conf/hadoop/hdfs-site.xml
        checkAndCopyFile $TDH_CLIENT_HOME/conf/hadoop/yarn-site.xml
        checkAndCopyFile $TDH_CLIENT_HOME/conf/hadoop/mapred-site.xml
        checkAndCopyFile $TDH_CLIENT_HOME/conf/hadoop/oauth2-configuration.yml
        checkAndCopyFile $TDH_CLIENT_HOME/conf/inceptor/hive-site.xml
        checkAndCopyFile $TDH_CLIENT_HOME/conf/inceptor/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hiveserver2-site.xml
        checkAndCopyFile $TDH_CLIENT_HOME/conf/inceptor/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hivemetastore-site.xml

        checkAndCopyFile $TDH_CLIENT_HOME/conf/hadoop/hadoop-env.sh
    elif [ -d "/etc/hadoop/conf" ]
    then
        mkdir -p ${KYLIN_HOME}/hadoop_conf
        # CDH/HDP platform

        checkAndCopyFile /etc/hadoop/conf/core-site.xml
        checkAndCopyFile /etc/hadoop/conf/hdfs-site.xml
        checkAndCopyFile /etc/hadoop/conf/yarn-site.xml
        checkAndCopyFile /etc/hive/conf/hive-site.xml
        checkAndCopyFile /etc/hadoop/conf/mapred-site.xml
        checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hiveserver2-site.xml
        checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hivemetastore-site.xml

        checkAndCopyFile /etc/hadoop/conf/topology.py
        checkAndCopyFile /etc/hadoop/conf/topology.map
        checkAndCopyFile /etc/hadoop/conf/ssl-client.xml
        checkAndCopyFile /etc/hadoop/conf/hadoop-env.sh
    elif [[ $(hadoop version 2>/dev/null) == *"mapr"* ]]
    then
        mkdir -p ${kylin_hadoop_conf_dir}

        checkAndCopyFile ${MAPR_HOME}/hadoop/hadoop-*/etc/hadoop/hdfs-site.xml
        checkAndCopyFile ${MAPR_HOME}/hadoop/hadoop-*/etc/hadoop/core-site.xml
        checkAndCopyFile ${MAPR_HOME}/hadoop/hadoop-*/etc/hadoop/yarn-site.xml
        checkAndCopyFile ${MAPR_HOME}/hive/hive-*/conf/hive-site.xml
        checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hiveserver2-site.xml
        checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hivemetastore-site.xml
    else
        # APACHE HADOOP platform
        APACHE_HADOOP_CONF_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.env.apache-hadoop-conf-dir`
        if [ -n "${APACHE_HADOOP_CONF_DIR}" ]; then
            APACHE_HIVE_CONF_DIR=`$KYLIN_HOME/bin/get-properties.sh kylin.env.apache-hive-conf-dir`
            if [ -z "${APACHE_HIVE_CONF_DIR}" ]; then
                quit "ERROR: Please set kylin.env.apache-hive-conf-dir in kylin.properties."
            fi

            mkdir -p ${KYLIN_HOME}/hadoop_conf

            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/core-site.xml
            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/hdfs-site.xml
            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/yarn-site.xml
            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/mapred-site.xml

            checkAndCopyFile $APACHE_HIVE_CONF_DIR/hive-site.xml

            sed -i -r "/hive.metastore.schema.verification/I{n; s/true/false/}" ${KYLIN_HOME}/hadoop_conf/hive-site.xml

            checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hiveserver2-site.xml
            checkAndCopyFile ${KYLIN_HOME}/hadoop_conf/hive-site.xml ${KYLIN_HOME}/hadoop_conf/hivemetastore-site.xml

            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/topology.py
            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/topology.map
            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/ssl-client.xml
            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/hadoop-env.sh
            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/cmt.xml
            checkAndCopyFile $APACHE_HADOOP_CONF_DIR/mountTable.xml

            mysql_connector_jar_dir="${KYLIN_HOME}/lib/ext/"
            if [ -z "$(find ${mysql_connector_jar_dir} -maxdepth 1 -name 'mysql-connector-*.jar')" ]; then
              rm -rf ${KYLIN_HOME}/hadoop_conf
              echo "The mysql connector jar is missing, please place it in the ${KYLIN_HOME}/lib/ext directory."
              exit 1
            fi
            cp -rf ${mysql_connector_jar_dir}/mysql-connector-*.jar ${KYLIN_HOME}/spark/jars/
        else
          if [ -f "${kylin_hadoop_conf_dir}/hdfs-site.xml" ]
          then
              echo "Hadoop conf directory currently generated based on manual mode."
          else
              echo "Missing hadoop conf files. Please contact Kyligence technical support for more details."
              exit 1
          fi
        fi
    fi

    # Ensure krb5.conf underlying hadoop_conf
    if [[ ${KERBEROS_ENABLED} == "true" ]];then
          checkExactlyAndCopyFile $KYLIN_HOME/conf/$KYLIN_KRB5CONF
    fi

    if [ -d ${KYLIN_HOME}/hadoop_conf_override ]
    then
        cp -rf ${KYLIN_HOME}/hadoop_conf_override/hive-site.xml ${kylin_hadoop_conf_dir}
    fi
}

function checkAndCopyFile() {
    source_file=$1
    if [[ -f ${source_file} ]]; then
        if [[ -n $2 ]]; then
            dst_dir=$2
        else
            dst_dir=${kylin_hadoop_conf_dir}
        fi
        cp -rf ${source_file} ${dst_dir}
    fi
}

# exit if the source file not exist
function checkExactlyAndCopyFile() {
    source_file=$1
    if [[ ! -f ${source_file} ]]; then
        echo "cannot find file ${source_file}, please check the existence of it"
        exit 1
    else
        checkAndCopyFile "$@"
    fi
}

# KE-9142 FI hive-site.xml is lack of some configuration
function checkAndCopyFIHiveSite() {

    checkAndCopyFile $FI_ENV_PLATFORM/Hive/config/hive-site.xml
    checkAndCopyFile $FI_ENV_PLATFORM/Hive/config/hivemetastore-site.xml
    hivesite_file=${kylin_hadoop_conf_dir}/hive-site.xml
    hivemeta_file=${kylin_hadoop_conf_dir}/hivemetastore-site.xml

    command -v xmllint > /dev/null || echo "ERROR: Command 'xmllint' is not accessible. Please install xmllint."
    if [[ -f ${hivemeta_file} ]] && [[ -f ${hivesite_file} ]]; then
        formartXML $hivemeta_file
        formartXML $hivesite_file
        metastore=$(echo "cat //configuration" |xmllint --shell $hivemeta_file| sed '/^\/ >/d'| sed '/configuration>/d')
        clean_content=$(echo $metastore | sed 's/\//\\\//g')
        if [[ -n $clean_content ]]; then
            sed -i "/<\/configuration.*>/ s/.*/${clean_content}&/" $hivesite_file
            formartXML $hivesite_file
        fi
    fi

    # Spark need hive-site.xml in FI
    checkAndCopyFile $hivesite_file ${SPARK_HOME}/conf
}

function formartXML() {
    file=$1
    if [[ -f ${file} ]]; then
        xmllint --format "$file" > "$file.xmlbak"
        if [[ $? == 0 ]]; then
            mv "$file.xmlbak" "$file"
        else
            echo "ERROR:  $file format error.Please check it."
        fi
    fi
}

if [[ "$kylin_hadoop_conf_dir" == "" ]]
then
    verbose Retrieving hadoop config dir...

    export kylin_hadoop_conf_dir=${KYLIN_HOME}/hadoop_conf

    export HADOOP_CONF_DIR=${KYLIN_HOME}/hadoop_conf

    if [[ ${KYLIN_ENV_CHANNEL} == "on-premises" || -z ${KYLIN_ENV_CHANNEL} ]]; then
        fetchKylinHadoopConf
    else
        fetchCloudHadoopConf
    fi
fi

