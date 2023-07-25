#!/bin/bash

#
# /*
#  * Licensed to the Apache Software Foundation (ASF) under one
#  * or more contributor license agreements.  See the NOTICE file
#  * distributed with this work for additional information
#  * regarding copyright ownership.  The ASF licenses this file
#  * to you under the Apache License, Version 2.0 (the
#  * "License"); you may not use this file except in compliance
#  * with the License.  You may obtain a copy of the License at
#  *
#  *     http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  */
#

## Because Apache Infra has limit size of a single file for download site,
## current the limit is 650MB. The release manager have to remove spark binary,
## so Kylin end user should execute this script to download and unzip Spark binary.
## After remove spark binary, Kylin binary is 476MB in 2023/03/09.
## Check details at https://issues.apache.org/jira/browse/INFRA-24053

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

if [[ -d ${KYLIN_HOME}/spark ]]; then
    echo "Spark already exists, please check."
    exit 1
fi

spark_version_in_binary=3.2.0-kylin-4.6.9.0
spark_pkg_name=spark-newten-"`echo ${spark_version_in_binary}| sed "s/-kylin//g"`"
spark_pkg_file_name="${spark_pkg_name}.tgz"

echo "spark_pkg_file_name : "${spark_pkg_file_name}
wget --directory-prefix=${KYLIN_HOME} https://s3.cn-north-1.amazonaws.com.cn/download-resource/kyspark/${spark_pkg_file_name} || echo "Download spark failed"

mkdir -p ${KYLIN_HOME}/${spark_pkg_name}
tar -zxf ${spark_pkg_file_name} -C ${spark_pkg_name} --strip-components 1 || { exit 1; }
mv ${spark_pkg_name} spark

# Remove unused components in Spark
rm -rf ${KYLIN_HOME}/spark/lib/spark-examples-*
rm -rf ${KYLIN_HOME}/spark/examples
rm -rf ${KYLIN_HOME}/spark/data
rm -rf ${KYLIN_HOME}/spark/R
rm -rf ${KYLIN_HOME}/spark/hive_1_2_2

# Temp fix of "Cannot find catalog plugin class for catalog 'spark_catalog': org.apache.spark.sql.delta.catalog.DeltaCatalog"
cp ${KYLIN_HOME}/server/jars/delta-core_2.12-2.0.2.jar ${KYLIN_HOME}/spark/jars/
cp -r ${KYLIN_HOME}/server/jars/alluxio-shaded-client-*.jar ${KYLIN_HOME}/spark/jars/
cp -r ${KYLIN_HOME}/server/jars/kylin-soft-affinity-cache-*.jar ${KYLIN_HOME}/spark/jars/
cp -r ${KYLIN_HOME}/server/jars/kylin-external-guava*.jar ${KYLIN_HOME}/spark/jars/
if [[ -n `find ${KYLIN_HOME}/server/jars/ -name "libthrift-*.jar"` ]]; then
  rm -f ${KYLIN_HOME}/spark/jars/libthrift-*.jar
  cp -rf ${KYLIN_HOME}/server/jars/libthrift-*.jar ${KYLIN_HOME}/spark/jars/
fi

if [ ! -f ${KYLIN_HOME}/"hive_1_2_2.tar.gz" ]
then
    echo "no binary file found"
    wget --directory-prefix=${KYLIN_HOME} https://s3.cn-north-1.amazonaws.com.cn/download-resource/kyspark/hive_1_2_2.tar.gz || echo "Download hive1 failed"
else
    if [ `calMd5 ${KYLIN_HOME}/hive_1_2_2.tar.gz | awk '{print $1}'` != "e8e86e086fb7e821d724ad0c19457a36" ]
    then
        echo "md5 check failed"
        rm ${KYLIN_HOME}/hive_1_2_2.tar.gz
        wget --directory-prefix=${KYLIN_HOME} https://s3.cn-north-1.amazonaws.com.cn/download-resource/kyspark/hive_1_2_2.tar.gz  || echo "Download hive1 failed"
    fi
fi
tar -zxf hive_1_2_2.tar.gz -C ${KYLIN_HOME}/spark/ || { exit 1; }
