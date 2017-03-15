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

rm -rf build/spark

alias md5cmd="md5sum"
if [[ `uname -a` =~ "Darwin" ]]; then
    alias md5cmd="md5 -q"
fi

spark_version="1.6.3"
spark_pkg_md5="ce8a2e7529aac0f0175194061769dbd4"

if [ ! -f "build/spark-${spark_version}-bin-hadoop2.6.tgz" ]
then
    echo "no binary file found"
    wget --directory-prefix=build/ http://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop2.6.tgz || echo "Download spark failed"
else
    if [ `md5cmd build/spark-${spark_version}-bin-hadoop2.6.tgz | awk '{print $1}'` != "${spark_pkg_md5}" ]
    then
        echo "md5 check failed"
        rm build/spark-${spark_version}-bin-hadoop2.6.tgz
        wget --directory-prefix=build/ http://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop2.6.tgz || echo "Download spark failed"

    fi
fi
unalias md5cmd

tar -zxvf build/spark-${spark_version}-bin-hadoop2.6.tgz -C build/   || { exit 1; }
mv build/spark-${spark_version}-bin-hadoop2.6 build/spark

# Remove unused components in Spark
rm -rf build/spark/lib/spark-examples-*
rm -rf build/spark/examples
rm -rf build/spark/data
rm -rf build/spark/R
