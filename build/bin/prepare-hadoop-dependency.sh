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

source ${KYLIN_HOME}/bin/check-hadoop-env.sh


BYPASS=${SPARK_HOME}/jars/replace-jars-bypass

if [[ -f ${BYPASS} ]]
then
    return
fi

if [[ $SPARK_HOME != $KYLIN_HOME* ]]; then
  echo "Skip spark which not owned by kylin. SPARK_HOME is $SPARK_HOME and KYLIN_HOME is $KYLIN_HOME.
  Please download the correct version of Apache Spark, unzip it, rename it to 'spark' and put it in $KYLIN_HOME directory.
  Do not use the spark that comes with your hadoop environment.
  If your hadoop environment is cdh6.x, you need to do some additional operations in advance.
  Please refer to the link: https://cwiki.apache.org/confluence/display/KYLIN/Deploy+Kylin+4+on+CDH+6."
  return
fi

echo "Start replace hadoop jars under ${KYLIN_HOME}/spark/jars."

hadoop_lib=${KYLIN_HOME}/spark/jars

common_jars=
hdfs_jars=
mr_jars=
yarn_jars=
other_jars=

function cdh_replace_jars() {
    common_jars=$(find $cdh_mapreduce_path/../hadoop -maxdepth 2 \
    -name "hadoop-annotations-*.jar" -not -name "*test*" \
    -o -name "hadoop-auth-*.jar" -not -name "*test*" \
    -o -name "hadoop-common-*.jar" -not -name "*test*")

    hdfs_jars=$(find $cdh_mapreduce_path/../hadoop-hdfs -maxdepth 1 -name "hadoop-hdfs-*" -not -name "*test*" -not -name "*nfs*")

    mr_jars=$(find $cdh_mapreduce_path -maxdepth 1 \
    -name "hadoop-mapreduce-client-app-*.jar" -not -name "*test*"  \
    -o -name "hadoop-mapreduce-client-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-jobclient-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-shuffle-*.jar" -not -name "*test*" \
    -o -name "hadoop-mapreduce-client-core-*.jar" -not -name "*test*")

    yarn_jars=$(find $cdh_mapreduce_path/../hadoop-yarn -maxdepth 1 \
    -name "hadoop-yarn-api-*.jar" -not -name "*test*"  \
    -o -name "hadoop-yarn-client-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-common-*.jar" -not -name "*test*" \
    -o -name "hadoop-yarn-server-web-proxy-*.jar" -not -name "*test*")

    other_jars=$(find $cdh_mapreduce_path/../../jars -maxdepth 1 -name "htrace-core4*" || find $cdh_mapreduce_path/../hadoop -maxdepth 2 -name "htrace-core4*")

    if [[ $(is_cdh_6_x) == 1 ]]; then
        cdh6_jars=$(find ${cdh_mapreduce_path}/../../jars -maxdepth 1 \
        -name "woodstox-core-*.jar" -o -name "stax2-*.jar" -o -name "commons-configuration2-*.jar" -o -name "re2j-*.jar" )
    fi
}

function emr_replace_jars() {
    common_jars=$(find ${emr_spark_lib_path}/jars/ -maxdepth 1 \
    -name "hadoop-*.jar" -not -name "*test*" \
    -o -name "htrace-core4*" \
    -o -name "emr-spark-goodies*")

    other_jars=$(find ${hadoop_lib_path}/lib/ -maxdepth 1 \
    -name "woodstox-core-*.jar" \
    -o -name "stax2-api-3*.jar")

    lzo_jars=$(find ${hadoop_lib_path}/../hadoop-lzo/lib/ -maxdepth 1 \
    -name "hadoop-lzo-*.jar" )

    if [[ $(is_aws_emr_6) == 1 ]]; then
        emr6_jars=$(find ${emr_spark_lib_path}/jars/ -maxdepth 1 \
        -name "re2j-*.jar" -not -name "*test*" \
        -o -name "commons-configuration2-*" )
    fi
}

function hdi_replace_jars() {
    common_jars=$(find ${hdi3_flag_path}/../spark2-client/ -maxdepth 2 \
    -name "hadoop-*.jar" -not -name "*test*" \
    -o -name "azure-*.jar" -not -name "*test*" \
    -o -name "guava-*.jar")
    
    other_jars=$(find ${hdi3_flag_path}/../hadoop-client/ -maxdepth 2 \
    -name "microsoft-log4j-etwappender-*.jar")

    lzo_jars=$(find ${hdi3_flag_path}/../hadoop-client/ -maxdepth 2 \
    -name "hadoop-lzo-*.jar" )
}

if [ -d "$cdh_mapreduce_path" ]
then
    cdh_replace_jars
elif [[ $(is_aws_emr) == 1 ]]
then
    emr_replace_jars
elif [[ $(is_hdi_3_x) == 1 ]]
then
    hdi_replace_jars
else
    touch "${BYPASS}"
fi

jar_list="${common_jars} ${hdfs_jars} ${mr_jars} ${yarn_jars} ${other_jars} ${cdh6_jars} ${emr6_jars} ${lzo_jars}"

echo "Find platform specific jars:${jar_list}, will replace with these jars under ${SPARK_HOME}/jars."

if [[ $(is_aws_emr_6) == 1 ]]; then
  find ${SPARK_HOME}/jars -name "hive-exec-*.jar" -exec rm -f {} \;
  hive_jars=$(find ${emr_spark_lib_path}/jars/ -maxdepth 1 -name "hive-exec-*.jar")
  cp ${hive_jars} ${SPARK_HOME}/jars
  configuration_jars=$(find ${emr_spark_lib_path}/../ -name "commons-configuration-1.10*.jar")
  cp ${configuration_jars} ${KYLIN_HOME}/lib
fi

if [[ $(is_cdh_6_x) == 1 ]]; then
   if [ -d "${KYLIN_HOME}/bin/hadoop3_jars/cdh6" ]; then
     find ${SPARK_HOME}/jars -name "hive-exec-*.jar" -exec rm -f {} \;
     echo "Copy jars from ${KYLIN_HOME}/bin/hadoop3_jars/cdh6"
     cp ${KYLIN_HOME}/bin/hadoop3_jars/cdh6/*.jar ${SPARK_HOME}/jars
   fi
fi

if [ ! -f ${BYPASS} ]; then
   find ${SPARK_HOME}/jars -name "htrace-core-*" -exec rm -rf {} \;
   find ${SPARK_HOME}/jars -name "hadoop-*.jar" -exec rm -f {} \;
fi

for jar_file in ${jar_list}
do
    `cp ${jar_file} ${SPARK_HOME}/jars`
done

if [[ (${is_emr} == 1) || ($(is_cdh_6_x) == 1)]]; then
   log4j_jars=$(find ${SPARK_HOME}/jars/ -maxdepth 2 -name "slf4j-*.jar")
   cp ${log4j_jars} ${KYLIN_HOME}/ext
fi

if [ $(is_hdi_3_x) == 1 ]; then
   if [[ -f ${KYLIN_HOME}/tomcat/webapps/kylin.war ]]; then
          if [[ ! -d ${KYLIN_HOME}/tomcat/webapps/kylin ]]
          then
             mkdir ${KYLIN_HOME}/tomcat/webapps/kylin
          fi
          mv ${KYLIN_HOME}/tomcat/webapps/kylin.war ${KYLIN_HOME}/tomcat/webapps/kylin
          cd ${KYLIN_HOME}/tomcat/webapps/kylin
          jar -xf ${KYLIN_HOME}/tomcat/webapps/kylin/kylin.war
          if [[ -f ${KYLIN_HOME}/tomcat/webapps/kylin/WEB-INF/lib/guava-14.0.jar ]]
          then
             echo "Remove ${KYLIN_HOME}/tomcat/webapps/kylin/WEB-INF/lib/guava-14.0.jar to avoid version conflicts"
             rm -rf ${KYLIN_HOME}/tomcat/webapps/kylin/WEB-INF/lib/guava-14.0.jar
             rm -rf ${KYLIN_HOME}/tomcat/webapps/kylin/kylin.war
             cd ${KYLIN_HOME}/
          fi
   fi
   find ${SPARK_HOME}/jars -name "guava-14*.jar" -exec rm -f {} \;
   echo "Upload spark jars to HDFS"
   hdfs dfs -test -d /spark2_jars
   if [ $? -eq 1 ]; then
      hdfs dfs -mkdir /spark2_jars
   fi
   hdfs dfs -put ${SPARK_HOME}/jars/* /spark2_jars
fi

# Remove all spaces
jar_list=${jar_list// /}

if [[ (-z "${jar_list}") && (! -f ${BYPASS}) ]]
then
    echo "Please confirm that the corresponding hadoop jars have been replaced. The automatic replacement program cannot be executed correctly."
else
    touch "${BYPASS}"
fi

echo "Done hadoop jars replacement under ${SPARK_HOME}/jars."
