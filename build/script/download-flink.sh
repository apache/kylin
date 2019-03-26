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

rm -rf build/flink

alias md5cmd="md5sum"
if [[ `uname -a` =~ "Darwin" ]]; then
    alias md5cmd="md5 -q"
fi

flink_version="1.7.2"
scala_version="2.11"
flink_pkg_md5="e0b5ce7f6352009c74b6c369f5872a5a"
guava_dependency_version="14.0.1"
jersey_version="1.9"

if [ ! -f "build/flink-${flink_version}-bin-hadoop27-scala_${scala_version}.tgz" ]; then
    echo "no binary file found"
    wget --directory-prefix=build/ http://archive.apache.org/dist/flink/flink-${flink_version}/flink-${flink_version}-bin-hadoop27-scala_${scala_version}.tgz || echo "Download flink failed"
else
    if [ `md5cmd build/ | awk '{print $1}'` != "${flink_pkg_md5}" ]; then
        echo "md5 check failed"
        rm build/flink-${flink_version}-bin-hadoop27-scala_${scala_version}.tgz
        wget --directory-prefix=build/ http://archive.apache.org/dist/flink/flink-${flink_version}/flink-${flink_version}-bin-hadoop27-scala_${scala_version}.tgz || echo "Download flink failed"
    fi
fi
unalias md5cmd

tar -zxvf build/flink-${flink_version}-bin-hadoop27-scala_${scala_version}.tgz -C build/   || { exit 1; }
mv build/flink-${flink_version} build/flink

# Remove unused components in Flink
rm -f build/flink/lib/flink-python*
rm -rf build/flink/examples
rm -rf build/flink/opt

# Download some dependencies
wget --directory-prefix=build/flink/lib/ http://central.maven.org/maven2/com/google/guava/guava/${guava_dependency_version}/guava-${guava_dependency_version}.jar || echo "Download guava dependency failed."
wget --directory-prefix=build/flink/lib/ http://central.maven.org/maven2/org/apache/flink/flink-hadoop-compatibility_${scala_version}/${flink_version}/flink-hadoop-compatibility_${scala_version}-${flink_version}.jar || echo "Download flink-hadoop-compatibility dependency failed."
wget --directory-prefix=build/flink/lib/ http://central.maven.org/maven2/com/sun/jersey/jersey-core/${jersey_version}/jersey-core-${jersey_version}.jar || echo "Download jersey-core dependency failed."