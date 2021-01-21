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

# check https://cwiki.apache.org/confluence/display/KYLIN/Deploy+Kylin+4+on+CDH+6

BYPASS=${KYLIN_HOME}/spark/jars/replace-jars-bypass
cdh_mapreduce_path="/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce"
hadoop_lib_path="/usr/lib/hadoop"

if [ -f ${BYPASS} ]; then
  exit 0
fi

if [ ! -d "$KYLIN_HOME/spark" ]; then
  echo "Skip spark which not owned by kylin. SPARK_HOME is $SPARK_HOME and KYLIN_HOME is $KYLIN_HOME ."
  exit 0
fi

echo "Start replacing hadoop jars under ${SPARK_HOME}/jars."

function check_cdh_hadoop() {
  # hadoop-common-3.0.0-cdh6.2.0.jar
  hadoop_common_file=$(find ${cdh_mapreduce_path}/../hadoop/ -maxdepth 1 -name "hadoop-common-*.jar" -not -name "*test*" | tail -1)
  cdh_version=${hadoop_common_file##*-}
  if [[ "${cdh_version}" == cdh6.* ]]; then
    export is_cdh6=1
  else
    export is_cdh6=0
  fi
  if [[ "${cdh_version}" == cdh5.* ]]; then
    export is_cdh5=1
  else
    export is_cdh5=0
  fi
}

function check_aws_emr() {
  if [ ! -d $hadoop_lib_path ]; then
    return 0
  fi

  # hadoop-common-3.2.1-amzn-0.jar
  hadoop_common_file=$(find $hadoop_lib_path -maxdepth 1 -name "hadoop-common-*.jar" -not -name "*test*" | tail -1)
  emr_version_1=${hadoop_common_file##*common-}
  echo $emr_version_1
  arrVersion=(${emr_version_1//-/ })

  if [[ "${arrVersion[0]}" == 3.* && "${arrVersion[1]}" == *amzn* ]]; then
    export is_emr6=1
  else
    export is_emr6=0
  fi

  if [[ "${arrVersion[0]}" == 2.* && "${arrVersion[1]}" == *amzn* ]]; then
    export is_emr5=1
  else
    export is_emr5=0
  fi
}

check_cdh_hadoop
check_aws_emr

common_jars=
hdfs_jars=
mr_jars=
yarn_jars=
other_jars=

if [ $is_cdh6 == 1 ]; then
  common_jars=$(find $cdh_mapreduce_path/../hadoop -maxdepth 2 \
    -name "hadoop-annotations-*.jar" -not -name "*test*" \
    -o -name "hadoop-auth-*.jar" -not -name "*test*" \
    -o -name "hadoop-common-*.jar" -not -name "*test*")

  hdfs_jars=$(find $cdh_mapreduce_path/../hadoop-hdfs -maxdepth 1 -name "hadoop-hdfs-*" -not -name "*test*" -not -name "*nfs*")

  mr_jars=$(find $cdh_mapreduce_path -maxdepth 1 \
    -name "hadoop-mapreduce-client-app-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-jobclient-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-shuffle-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-core-*.jar" -not -name "*test*")

  yarn_jars=$(find $cdh_mapreduce_path/../hadoop-yarn -maxdepth 1 \
    -name "hadoop-yarn-api-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-client-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-web-proxy-*.jar" -not -name "*test*")

  other_jars=$(find $cdh_mapreduce_path/../../jars -maxdepth 1 -name "htrace-core4*" || find $cdh_mapreduce_path/../hadoop -maxdepth 2 -name "htrace-core4*")

  if [[ $is_cdh6 == 1 ]]; then
    cdh6_jars=$(find ${cdh_mapreduce_path}/../../jars -maxdepth 1 \
      -name "woodstox-core-*.jar" -o -name "commons-configuration2-*.jar" -o -name "re2j-*.jar")
  fi
fi

jar_list="${common_jars} ${hdfs_jars} ${mr_jars} ${yarn_jars} ${other_jars} ${cdh6_jars}"

echo "Find platform specific jars:${jar_list}, will replace with these jars under ${SPARK_HOME}/jars."

if [ $is_cdh6 == 1 ]; then
  find ${KYLIN_HOME}/spark/jars -name "hadoop-hdfs-*.jar" -exec rm -f {} \;
  find ${KYLIN_HOME}/spark/jars -name "hadoop-yarn-*.jar" -exec rm -f {} \;
  find ${KYLIN_HOME}/spark/jars -name "hadoop-mapreduce-*.jar" -exec rm -f {} \;
  find ${KYLIN_HOME}/spark/jars -name "hadoop-annotations-*.jar" -exec rm -f {} \;
  find ${KYLIN_HOME}/spark/jars -name "hadoop-auth-*.jar" -exec rm -f {} \;
  find ${KYLIN_HOME}/spark/jars -name "hadoop-client-*.jar" -exec rm -f {} \;
  find ${KYLIN_HOME}/spark/jars -name "hadoop-common-*.jar" -exec rm -f {} \;
  find ${KYLIN_HOME}/spark/jars -name "hive-exec-*.jar" -exec rm -f {} \;
  if [ -d "${KYLIN_HOME}/bin/hadoop3_jars/cdh6" ]; then
    echo "Copy jars from ${KYLIN_HOME}/bin/hadoop3_jars/cdh6"
    cp ${KYLIN_HOME}/bin/hadoop3_jars/cdh6/*.jar ${SPARK_HOME}/jars
  fi
fi

for jar_file in ${jar_list}; do
  $(cp ${jar_file} ${KYLIN_HOME}/spark/jars)
done

# Remove all spaces
jar_list=${jar_list// /}

if [ -z "${jar_list}" ]; then
  echo "Please confirm that the corresponding hadoop jars have been replaced. The automatic replacement program cannot be executed correctly."
else
  echo "Replace jars under SPARK_HOME/jars finished."
  touch ${BYPASS}
fi

echo "Done hadoop jars replacement under ${SPARK_HOME}/jars."