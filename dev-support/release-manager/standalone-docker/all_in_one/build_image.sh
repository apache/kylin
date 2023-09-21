#!/usr/bin/env bash

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
TAG=5.0-beta

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ${DIR} || exit
echo "build image in dir "${DIR}

echo "package kylin in local for building image"
if [[ ! -d ${DIR}/package/ ]]; then
    mkdir -p ${DIR}/package/
fi

# The official package didn't carry with Spark binary,
# So I download it in my laptop, uncompress, execute download-spark-user.sh and re-compress
#
# wget https://archive.apache.org/dist/kylin/apache-kylin-5.0.0-beta/apache-kylin-5.0.0-beta-bin.tar.gz -P ${DIR}/package/
# tar zxf apache-kylin-5.0.0-beta-bin.tar.gz
# cd apache-kylin-5.0.0-beta-bin
# bash sbin/download-spark-user.sh
# tar -czf apache-kylin-5.0.0-beta-bin.tar.gz apache-kylin-5.0.0-beta-bin

echo "start to build Hadoop docker image"
# docker build -f Dockerfile_hadoop -t hadoop3.2.1-all-in-one-for-kylin5 .
docker build -f Dockerfile_kylin -t apachekylin/apache-kylin-standalone:${TAG} .

docker image tag docker.io/apachekylin/apache-kylin-standalone:${TAG} apachekylin/apache-kylin-standalone:${TAG}
docker stop Kylin5-Machine
docker rm Kylin5-Machine

docker run -d \
  --name Kylin5-Machine \
  --hostname Kylin5-Machine \
  -m 8G \
  -p 7070:7070 \
  -p 8088:8088 \
  -p 50070:50070 \
  -p 8032:8032 \
  -p 8042:8042 \
  -p 2181:2181 \
  apachekylin/apache-kylin-standalone:${TAG}

docker logs --follow Kylin5-Machine
