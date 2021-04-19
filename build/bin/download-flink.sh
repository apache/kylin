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

source ${KYLIN_HOME:-"$(cd -P -- "$(dirname -- "$0")" && pwd -P)/../"}/bin/header.sh

if [ -d "${KYLIN_HOME}/flink" ]; then
    echo "Flink binary exists"
    exit 0;
else
    echo "Downloading flink package..."
fi

flink_package_dir=/tmp/flink_package

mkdir -p -- "${flink_package_dir}" && cd -P -- "${flink_package_dir}"

alias md5cmd="md5sum"
if [[ `uname -a` =~ "Darwin" ]]; then
    alias md5cmd="md5 -q"
fi

flink_version="1.11.1"
scala_version="2.11"
flink_shaded_version="10.0"
hadoop_version="2.7.5"
flink_pkg_md5="3b7aa59b44add1a0625737f6516e0929"
flink_shaded_hadoop_md5="4287a314bfb09a3dc957cbda3f91d7ca"

if [ ! -f "flink-${flink_version}-bin-scala_${scala_version}.tgz" ]; then
    echo "No binary file found, start to download package to ${flink_package_dir}"
    wget http://archive.apache.org/dist/flink/flink-${flink_version}/flink-${flink_version}-bin-scala_${scala_version}.tgz || echo "Download flink failed"
else
    if [ `md5cmd flink-${flink_version}-bin-scala_${scala_version}.tgz | awk '{print $1}'` != "${flink_pkg_md5}" ]; then
        echo "md5 check failed"
        rm flink-${flink_version}-bin-scala_${scala_version}.tgz
        wget http://archive.apache.org/dist/flink/flink-${flink_version}/flink-${flink_version}-bin-scala_${scala_version}.tgz || echo "Download flink failed"
    fi
fi

flink_shaded_hadoop_jar="flink-shaded-hadoop-2-uber-${hadoop_version}-${flink_shaded_version}.jar"
flink_shaded_hadoop_path="https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/${hadoop_version}-${flink_shaded_version}/${flink_shaded_hadoop_jar}"

if [ ! -f $flink_shaded_hadoop_jar ]; then
    echo "Start to download $flink_shaded_hadoop_jar"
    wget $flink_shaded_hadoop_path || echo "Download flink shaded hadoop jar failed"
else
    if [ `md5cmd $flink_shaded_hadoop_jar | awk '{print $1}'` != $flink_shaded_hadoop_md5 ]; then
        echo "md5 check failed"
        rm $flink_shaded_hadoop_jar
        wget $flink_shaded_hadoop_path || echo "Download flink shaded hadoop jar failed"
    fi
fi
unalias md5cmd

echo "Start to decompress package"
tar -zxvf flink-${flink_version}-bin-scala_${scala_version}.tgz || { exit 1; }
mv flink-${flink_version} flink
mv $flink_shaded_hadoop_jar flink/lib

# mv flink binary to KYLIN_HOME
mv flink ${KYLIN_HOME}

rm -rf ${flink_package_dir}

echo "Download flink binary done"
