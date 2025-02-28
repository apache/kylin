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
TAG=kylin5-dev
cd ..
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ${DIR} || exit
echo "build image in dir "${DIR}

echo "package kylin in local for building image"
if [[ ! -d ${DIR}/../package/ ]]; then
    mkdir -p ${DIR}/../package/
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
cp ../../tpch-benchmark/scripts/*.sql dev-docker
docker build -f dev-docker/Dockerfile_kylin_dev -t apachekylin/apache-kylin-standalone:${TAG} .
BUILD_RESULT=$?

if [ "$BUILD_RESULT" != "0" ]; then
  echo "Image build failed, please check"
  exit 1
fi
echo "Image build succeed"

docker image tag docker.io/apachekylin/apache-kylin-standalone:${TAG} apachekylin/apache-kylin-standalone:${TAG}

echo "Start this image locally, and push it to dockerhub later."

rm dev-docker/*.sql

docker stop kylin5-dev
docker rm kylin5-dev
docker run -d \
  --name kylin5-dev \
  --hostname Kylin5-Machine \
  --network host \
  -m 8G \
  --env OUTER_HOST="172.16.4.56" \
  apachekylin/apache-kylin-standalone:kylin5-dev

docker logs --follow kylin5-dev


#docker run -d \
#  --name kylin5-dev \
#  --hostname Kylin5-Machine \
#  -p 2181:2181 \
#  -p 3306:3306 \
#  -p 9000:9000 \
#  -p 9866:9866 \
#  -p 8032:8032 \
#  -p 9867:9867 \
#  -m 8G \
#  --env OUTER_HOST="127.0.0.1" \
#  apachekylin/apache-kylin-standalone:kylin5-dev
#
#docker logs --follow kylin5-dev
#
#docker save apachekylin/apache-kylin-standalone:kylin5-dev | bzip2 | \
#   ssh kylin@worker-03 'bunzip2 | docker load'