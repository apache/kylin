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

cdh_mapreduce_path="/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce"
hadoop_lib_path="/usr/lib/hadoop"
emr_spark_lib_path="/usr/lib/spark"
hdi3_flag_path="/usr/hdp/current/hdinsight-zookeeper"

cdh_version=`hadoop version | head -1 | awk -F '-' '{print $2}'`

function is_cdh_6_x() {
    if [ -d ${cdh_mapreduce_path}/../hadoop/ ]; then
       hadoop_common_file=`find ${cdh_mapreduce_path}/../hadoop/ -maxdepth 1 -name "hadoop-common-*.jar" -not -name "*test*" | tail -1`
       cdh_version=${hadoop_common_file##*-}

       if [[ "${cdh_version}" == cdh6.* ]]; then
          echo 1
          return 1
       fi
    fi
    echo 0
    return 0
}

hdp_hadoop_path=$HDP_HADOOP_HOME
if [[ -z ${hdp_hadoop_path} ]]
then
    if [[ -d "/usr/hdp/current/hadoop-client" ]]; then
        hdp_hadoop_path="/usr/hdp/current/hadoop-client"
    fi
fi

if [ -d $hadoop_lib_path ]; then
   # hadoop-common-3.2.1-amzn-0.jar
   hadoop_common_file=$(find $hadoop_lib_path -maxdepth 1 -name "hadoop-common-*.jar" -not -name "*test*" | tail -1)
   emr_version_1=${hadoop_common_file##*common-}
   arrVersion=(${emr_version_1//-/ })
fi

function is_aws_emr() {
  if [[ "${arrVersion[1]}" == *amzn* ]]; then
     echo 1
     return 1
  fi
  echo 0
  return 0
}

function is_aws_emr_6() {
  if [[ "${arrVersion[0]}" == 3.* && "${arrVersion[1]}" == *amzn* ]]; then
      echo 1
      return 1
  fi
  echo 0
  return 0
}

function is_hdi_3_x() {
  # get hdp_version
  if [ -z "${hdp_version}" ]; then
      hdp_version=`/bin/bash -x hadoop 2>&1 | sed -n "s/\(.*\)export HDP_VERSION=\(.*\)/\2/"p`
      verbose "hdp_version is ${hdp_version}"
  fi

  if [[ -d "/usr/hdp/current/hdinsight-zookeeper" && $hdp_version == "2"* ]];then
     echo 1
     return 1
  fi

  echo 0
  return 0
}