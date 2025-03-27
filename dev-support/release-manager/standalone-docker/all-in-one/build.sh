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


TAG=5.0.2-GA

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ${DIR} || exit
echo "build image in dir "${DIR}

echo "package kylin in local for building image"
if [[ ! -d ${DIR}/package/ ]]; then
    mkdir -p ${DIR}/package/
fi

# The official apache kylin package has no Spark binary included, prepare manually with following steps:
# 1. Download apache kylin binary and extract
# 2. Execute sbin/download-spark-user.sh and should have a new spark folder at the root of kylin dir
# 3. Re-compress kylin folder and put it to the package dir for Dockerfile use
#
# wget https://github.com/apache/kylin/releases/download/kylin-5.0.2/apache-kylin-5.0.2-bin.tar.gz -P ${DIR}/package/
# tar zxf apache-kylin-5.0.0-bin.tar.gz
# cd apache-kylin-5.0.2-bin
# bash sbin/download-spark-user.sh
# tar -czf apache-kylin-5.0.2-bin.tar.gz apache-kylin-5.0.2-bin
# Notice - For mac tar command use: tar czf apache-kylin-5.0.2-bin.tar.gz --no-mac-metadata apache-kylin-5.0.2-bin
# to avoid AppleDouble format hidden files inside the compressed file

echo "start to build kylin standalone docker image"
docker build . -t apachekylin/apache-kylin-standalone:${TAG}

BUILD_RESULT=$?
if [ "$BUILD_RESULT" != "0" ]; then
  echo "Image build failed, please check"
  exit 1
fi
echo "Image build succeed"
