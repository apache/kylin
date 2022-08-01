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

cdh_mapreduce_path=$CDH_MR2_HOME
cdh_mapreduce_path_first="/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce"
cdh_mapreduce_path_second="/usr/lib/hadoop-mapreduce"
cdh_hadoop_lib_path="/opt/cloudera/parcels/CDH/lib/hadoop"
cdh_version=`hadoop version | head -1 | awk -F '-' '{print $2}'`

function quit {
    echo "$@"
    if [[ -n "${QUIT_MESSAGE_LOG}" ]]; then
        echo `setColor 31 "$@"` >> ${QUIT_MESSAGE_LOG}
    fi
    exit 1
}

function get_cdh_version_from_common_jar() {
    hadoop_common_path=`find ${cdh_mapreduce_path}/../hadoop/ -maxdepth 1 -name "hadoop-common-*.jar" -not -name "*test*" | tail -1`
    hadoop_common_jar=${hadoop_common_path##*-}
    echo "${hadoop_common_jar%.*}"
}

function set_cdh_mapreduce_path_with_hadoop_version_command() {
    cdh_mapreduce_path=${cdh_mapreduce_path_first}
    cdh_version_from_jar=`get_cdh_version_from_common_jar`

     # check cdh version from hadoop version command equals cdh version from common jar
    if [[ "${cdh_version}" == "${cdh_version_from_jar}" ]]; then
        return
    fi

    cdh_mapreduce_path=${cdh_mapreduce_path_second}
    cdh_version_from_jar=`get_cdh_version_from_common_jar`
    if [[ "${cdh_version}" != "${cdh_version_from_jar}" ]]; then
        quit "Get cdh mapreduce path failed, please set CDH_MR2_HOME environment variable."
    fi
}

if [[ -z ${cdh_mapreduce_path} ]]
then
    if [[ "${cdh_version}" == cdh* ]]; then
        set_cdh_mapreduce_path_with_hadoop_version_command
    else
        if [[ -d "${cdh_mapreduce_path_first}" ]]; then
            cdh_mapreduce_path=${cdh_mapreduce_path_first}
        else
            cdh_mapreduce_path=${cdh_mapreduce_path_second}
        fi
    fi
fi

function is_cdh_6_x() {
    hadoop_common_file=`find ${cdh_mapreduce_path}/../hadoop/ -maxdepth 1 -name "hadoop-common-*.jar" -not -name "*test*" | tail -1`
    cdh_version=${hadoop_common_file##*-}

    if [[ "${cdh_version}" == cdh6.* ]]; then
        echo 1
        return 1
    fi

    echo 0
    return 0
}

hdp_hadoop_path=$HDP_HADOOP_HOME
if [[ -z ${hdp_hadoop_path} ]]
then
    if [[ -d "/usr/hdp/current/hadoop-client" ]]; then
        hdp_hadoop_path="/usr/hdp/current/hadoop-client"
    else
        hdp_hadoop_path="/usr/hdp/*/hadoop"
    fi
fi

function is_hdp_3_x() {
    hadoop_common_file=$(find ${hdp_hadoop_path}/ -maxdepth 1 -name hadoop-common-*.jar -not -name "*test*" | tail -1)
    hdp_version=${hadoop_common_file##*/}

    if [[ "${hdp_version}" == hadoop-common-3.* ]]; then
        echo 1
        return 1
    fi

    echo 0
    return 0
}

function is_cdh_7_x() {
    hdp_version=`hadoop version | grep "CDH-7.*" | awk -F"/" '{print $5}'`

    if [[ -n "${hdp_version}" ]]; then
        echo 1
        return 1
    fi

    echo 0
    return 0
}

function is_hdp_2_6() {
    hadoop_common_file="`find ${hdp_hadoop_path}/ -maxdepth 1 -name hadoop-common-*.jar -not -name *test* | tail -1`"
    hdp_version=${hadoop_common_file##*/}

    if [[ "${hdp_version}" == hadoop-common-2*2.6.* ]]; then
        echo 1
        return 1
    fi

    echo 0
    return 0
}

function is_fi_c90() {
    ## FusionInsight platform C70/C90.
    if [[ -n "$BIGDATA_CLIENT_HOME" ]]; then
        hadoop_common_file="`find ${BIGDATA_CLIENT_HOME}/HDFS/hadoop/share/hadoop/common/ -maxdepth 1 -name hadoop-common-*.jar -not -name *test* | tail -1`"
        fi_version=${hadoop_common_file##*/}

        if [[ "${fi_version}" == hadoop-common-3.1* ]]; then
            echo 1
            return 1
        fi
    fi

    echo 0
    return 0
}

tdh_client_path=$TDH_CLIENT_HOME
function is_tdh() {
  if [[ -n "$tdh_client_path" ]]; then
    if [[ -f "${tdh_client_path}/init.sh" ]]; then
      echo 1
      return 1
    fi
    quit "Please check TDH_CLIENT_HOME is correct, it contains init.sh file."
    echo 0
    return 0
  fi

  echo 0
  return 0
}

function prepare_hadoop_conf_jars(){
  if [[ $(is_cdh_7_x) == 1 ]]; then
    find ${SPARK_HOME}/jars -name "guava-*.jar" -exec rm -rf {} \;
    find ${KYLIN_HOME}/server/jars -name "guava-*.jar" -exec rm -rf {} \;

    guava_jars=$(find ${cdh_hadoop_lib_path}/client/ -maxdepth 1 \
    -name "guava-*.jar" \
    -o -name "failureaccess.jar")

    cp ${guava_jars} "${SPARK_HOME}"/jars
    cp ${guava_jars} "${KYLIN_HOME}"/server/jars
    hadoop_conf_cdh7x_jars=$(find ${cdh_hadoop_lib_path}/client/ -maxdepth 1 -name "commons-configuration2-*.jar")
    cp ${hadoop_conf_cdh7x_jars} ${SPARK_HOME}/jars
  fi
}