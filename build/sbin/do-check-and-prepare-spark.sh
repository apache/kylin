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

source ${KYLIN_HOME}/sbin/replace-jars-under-spark.sh

function configureMaprSparkSql() {
    # copy hive-conf/hive-site.xml to $SPARK_HOME/conf/ and delete "hive.execution.engine" property
    if [ -z ${kylin_hadoop_conf_dir} ]; then
        source ${KYLIN_HOME}/sbin/prepare-hadoop-conf-dir.sh
    fi

    hive_conf_spark_path="${SPARK_HOME}/conf/hive-site.xml"
    if [ -f ${hive_conf_spark_path} ]; then
        rm -f ${hive_conf_spark_path}
    fi
    cp ${kylin_hadoop_conf_dir}/hive-site.xml ${hive_conf_spark_path}
    match_line=`grep -n "<name>hive.execution.engine</name>" ${hive_conf_spark_path}|cut -d ":" -f 1`
    if [ -n "${match_line}" ]; then
        start_line=$(($match_line-1))
        end_line=$(($match_line+2))
        sed -i "${start_line}"','"${end_line}"'d' ${hive_conf_spark_path}
    fi

    # configure "spark.yarn.dist.files" in spark-defaults.conf
    config_spark_default_str="spark.yarn.dist.files"
    spark_defaults_conf_path="${SPARK_HOME}/conf/spark-defaults.conf"
    if [ ! -f ${spark_defaults_conf_path} ]; then
        touch ${spark_defaults_conf_path}
    fi
    match_spark_default_line=`grep -n "${config_spark_default_str}" ${spark_defaults_conf_path}|cut -d ":" -f 1`
    if [ -n "${match_spark_default_line}" ]; then
        sed -i "${match_spark_default_line}"'d' ${spark_defaults_conf_path}
    fi
    echo "${config_spark_default_str}     ${hive_conf_spark_path}"  >> ${spark_defaults_conf_path}

    # export SAPRK_HOME in spark-env.sh
    config_spark_env_str="export SPARK_HOME=${SPARK_HOME}"
    spark_env_sh_path="${SPARK_HOME}/conf/spark-env.sh"
    if [ -f ${spark_env_sh_path} ]; then
        match_spark_env_line=`grep -n "${config_spark_env_str}" ${spark_env_sh_path}|cut -d ":" -f 1`
        if [ -n "${match_spark_env_line}" ]; then
            sed -i "${match_spark_env_line}"'d' ${spark_env_sh_path}
        fi
        echo "${config_spark_env_str}" >> ${spark_env_sh_path}
    fi
}

# check prepare spark

if [[ $(hadoop version 2>/dev/null) == *"mapr"* ]]
then
    SPARK_HDP_VERSION="mapr"
else
    if [ -f /etc/cloudversion ]
    then
        case $(cat /etc/cloudversion) in
            *"aliyun"*)
            {
                SPARK_HDP_VERSION="aliyun"
            }
            ;;
            *"azure"*)
            {
                SPARK_HDP_VERSION="azure"
            }
            ;;
            *"aws"*)
            {
                SPARK_HDP_VERSION="aws"
            }
            ;;
            *)
            {
                SPARK_HDP_VERSION="hadoop"
            }
        esac
    else
        SPARK_HDP_VERSION="hadoop"
    fi
fi
verbose "SPARK_HDP_VERSION is set to '${SPARK_HDP_VERSION}'"

## MapR is a different operation rule

if [ ! -f ${KYLIN_HOME}/spark/spark_hdp_version ]
then
    echo "hadoop" > ${KYLIN_HOME}/spark/spark_hdp_version
fi
if [[ $(cat ${KYLIN_HOME}/spark/spark_hdp_version) != *"${SPARK_HDP_VERSION}"* ]]
then
    cp -rf ${KYLIN_HOME}/spark ${KYLIN_HOME}/spark_backup
    case $SPARK_HDP_VERSION in
        "mapr")
        {
            if [[ ! -d "$SPARK_HOME" ]]
            then
                quit 'Please make sure SPARK_HOME has been set (export as environment variable first)'
            fi
            rm -rf ${SPARK_HOME}/jars/hadoop-*.jar
            rm -rf ${SPARK_HOME}/jars/zookeeper-*.jar
            rm -rf ${KYLIN_HOME}/server/jars/protobuf-java-*.jar
            rm -rf ${SPARK_HOME}/jars/protobuf-java*.jar

            hadoop_common_jars=$(find ${MAPR_HOME}/hadoop/hadoop*/share/hadoop/common/ -maxdepth 2 -name hadoop-*.jar -not -name "*test*" -not -name "*sources.jar")
            hadoop_hdfs_jars=$(find ${MAPR_HOME}/hadoop/hadoop*/share/hadoop/hdfs/ -maxdepth 2 -name hadoop-*.jar -not -name "*test*" -not -name "*sources.jar")
            hadoop_mapreduce_jars=$(find ${MAPR_HOME}/hadoop/hadoop*/share/hadoop/mapreduce/ -maxdepth 2 -name hadoop-*.jar -not -name "*test*" -not -name "*sources.jar")
            hadoop_yarn_jars=$(find ${MAPR_HOME}/hadoop/hadoop*/share/hadoop/yarn/ -maxdepth 2 -name hadoop-*.jar -not -name "*test*" -not -name "*sources.jar")
            zk_jars=$(find ${MAPR_HOME}/zookeeper/zookeeper*/  -maxdepth 2 -name zookeeper-*.jar -not -name "*test*")
            maprfs_jars=$(find ${MAPR_HOME}/hadoop/hadoop*/share/hadoop/common/lib/ -maxdepth 2 -name maprfs-*.jar -not -name "*test*")
            mapr_jars=$(find ${MAPR_HOME}/hadoop/hadoop*/share/hadoop/common/lib/ -maxdepth 2 -name mapr-*.jar -not -name "*test*")
            maprdb_jars=$(find ${MAPR_HOME}/hadoop/hadoop*/share/hadoop/common/lib/ -maxdepth 2 -name maprdb-*.jar -not -name "*test*")
            mapr_lib_jars=$(find ${MAPR_HOME}/lib/ -maxdepth 1 -name json-*.jar -o -name flexjson-*.jar -o -name maprutil*.jar -o -name baseutils*.jar -o -name libprotodefs*.jar -o -name kvstore*.jar)
            other_jars=$(find ${MAPR_HOME}/hadoop/hadoop*/share/hadoop/common/lib/ -maxdepth 2 -name htrace-core-*.jar -o -name aws-java-sdk-*.jar)
            protobuf_jars=$(find ${MAPR_HOME}/hadoop/hadoop*/share/hadoop/common/lib/ -maxdepth 2 -name protobuf-java-*.jar -not -name "*test*")

            cp ${hadoop_common_jars} ${SPARK_HOME}/jars/
            cp ${hadoop_hdfs_jars} ${SPARK_HOME}/jars/
            cp ${hadoop_mapreduce_jars} ${SPARK_HOME}/jars/
            cp ${hadoop_yarn_jars} ${SPARK_HOME}/jars/
            cp ${zk_jars} ${SPARK_HOME}/jars/
            cp ${maprfs_jars} ${SPARK_HOME}/jars/
            cp ${maprdb_jars} ${SPARK_HOME}/jars/
            cp ${mapr_jars} ${SPARK_HOME}/jars/
            cp ${other_jars} ${SPARK_HOME}/jars/
            cp ${mapr_lib_jars} ${SPARK_HOME}/jars/
            cp ${protobuf_jars} ${SPARK_HOME}/jars/
        }
        ;;
        "aliyun")
        {
            cp /usr/lib/hadoop-current/lib/hadoop-lzo-*.jar ${KYLIN_HOME}/spark/jars/
            cp /usr/lib/hadoop-current/share/hadoop/tools/lib/hadoop-aliyun-*.jar ${KYLIN_HOME}/spark/jars/
            cp /usr/lib/spark-current/jars/aliyun-sdk-oss-*.jar ${KYLIN_HOME}/spark/jars/
            cp /usr/lib/hadoop-current/share/hadoop/tools/lib/jdom-*.jar ${KYLIN_HOME}/spark/jars/
        }
        ;;
        "azure")
        {
            rm -rf ${KYLIN_HOME}/spark/jars/hadoop-*
            cp /usr/hdp/current/spark2-client/jars/hadoop-* ${KYLIN_HOME}/spark/jars/
            cp /usr/hdp/current/spark2-client/jars/azure* ${KYLIN_HOME}/spark/jars/
            cp /usr/hdp/current/spark2-client/jars/aws* ${KYLIN_HOME}/spark/jars/
            cp /usr/hdp/current/spark2-client/jars/hadoop-aws-*.jar ${KYLIN_HOME}/spark/jars/
        }
        ;;
        "aws")
        {
            rm -rf ${KYLIN_HOME}/spark/jars/hadoop-*
            cp /usr/lib/hadoop/hadoop*amzn*.jar ${KYLIN_HOME}/spark/jars/
            cp /usr/lib/hadoop-hdfs/hadoop*amzn*.jar ${KYLIN_HOME}/spark/jars/
            cp /usr/lib/hadoop-mapreduce/hadoop*amzn*.jar ${KYLIN_HOME}/spark/jars/
            cp /usr/lib/hadoop-yarn/hadoop*amzn*.jar ${KYLIN_HOME}/spark/jars/
        }
        ;;
    esac
    echo "${SPARK_HDP_VERSION}" > ${KYLIN_HOME}/spark/spark_hdp_version

    echo "${SPARK_HDP_VERSION} Spark jars exchange SUCCESS"
    echo "SPARK_HDP_VERSION save in PATH ${KYLIN_HOME}/spark/spark_hdp_version"
fi

export SPARK_HOME=${KYLIN_HOME}/spark
verbose "Export SPARK_HOME to ${KYLIN_HOME}/spark"

if [ "${SPARK_HDP_VERSION}" = "mapr" ]; then
    configureMaprSparkSql
fi