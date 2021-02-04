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

if [ -d "${KYLIN_HOME}/spark" ]
then
    echo "Spark binary exists"
    exit 0;
else
    echo "Downloading spark package..."
fi

spark_package_dir=/tmp/spark_package

mkdir -p -- "${spark_package_dir}" && cd -P -- "${spark_package_dir}"

alias md5cmd="md5sum"
if [[ `uname -a` =~ "Darwin" ]]; then
    alias md5cmd="md5 -q"
fi

spark_version="2.4.7"
spark_pkg_md5="76afb611aaac5721c9fa91fdc9defa99"

if [ ! -f "spark-${spark_version}-bin-hadoop2.7.tgz" ]
then
    echo "No binary file found, start to download package to ${spark_package_dir}"
    wget http://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop2.7.tgz|| echo "Download spark failed"
else
    if [ `md5cmd spark-${spark_version}-bin-hadoop2.7.tgz | awk '{print $1}'` != "${spark_pkg_md5}" ]
    then
        echo "md5 check failed"
        rm spark-${spark_version}-bin-hadoop2.7.tgz
        wget http://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop2.7.tgz || echo "Download spark failed"
    else
        echo "Spark package found in ${spark_package_dir}"
    fi
fi
unalias md5cmd

echo "Start to decompress package"
tar -zxvf spark-${spark_version}-bin-hadoop2.7.tgz  || { exit 1; }
mv spark-${spark_version}-bin-hadoop2.7 spark

# Remove unused components in Spark
rm -rf spark/lib/spark-examples-*
rm -rf spark/examples
rm -rf spark/data
rm -rf spark/R

# mv spark binary to KYLIN_HOME
mv spark ${KYLIN_HOME}

echo "Download spark binary done"

rm -rf ${spark_package_dir}
