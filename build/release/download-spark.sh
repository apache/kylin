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

dir=$(dirname ${0})
cd ${dir}/../..

source build/release/functions.sh

rm -rf build/spark

spark_version_pom=`mvn -f pom.xml help:evaluate -Dexpression=spark.version | grep -E '^[0-9]+\.[0-9]+\.[0-9]+' `
spark_pkg_name=spark-newten-"`echo ${spark_version_pom}| sed "s/-kylin//g"`"
spark_pkg_file_name="${spark_pkg_name}.tgz"

echo "spark_pkg_file_name : "${spark_pkg_file_name}

if [ ! -f "build/${spark_pkg_file_name}" ]
then
    echo "no binary file found"
    wget --directory-prefix=build/ https://s3.cn-north-1.amazonaws.com.cn/download-resource/kyspark/${spark_pkg_file_name} || echo "Download spark failed"
fi

mkdir -p  build/${spark_pkg_name}
tar -zxf build/${spark_pkg_file_name} -C build/${spark_pkg_name} --strip-components 1 || { exit 1; }
mv build/${spark_pkg_name} build/spark

# Remove unused components in Spark
rm -rf build/spark/lib/spark-examples-*
rm -rf build/spark/examples
rm -rf build/spark/data
rm -rf build/spark/R
rm -rf build/spark/hive_1_2_2

if [[ "${WITH_HIVE1}" != "0" ]]; then
    if [ ! -f "build/hive_1_2_2.tar.gz" ]
    then
        echo "no binary file found"
        wget --directory-prefix=build/ https://s3.cn-north-1.amazonaws.com.cn/download-resource/kyspark/hive_1_2_2.tar.gz || echo "Download hive1 failed"
    else
        if [ `calMd5 build/hive_1_2_2.tar.gz | awk '{print $1}'` != "e8e86e086fb7e821d724ad0c19457a36" ]
        then
            echo "md5 check failed"
            rm build/hive_1_2_2.tar.gz
            wget --directory-prefix=build/ https://s3.cn-north-1.amazonaws.com.cn/download-resource/kyspark/hive_1_2_2.tar.gz  || echo "Download hive1 failed"
        fi
    fi
    tar -zxf build/hive_1_2_2.tar.gz -C build/spark/ || { exit 1; }
fi
