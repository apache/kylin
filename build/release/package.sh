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

echo "Packing for Apache Kylin ..."

# Make share commands exist in environment
echo "BUILD STAGE 1 - Checking environment..."
checkCommandExists mvn
checkCommandExists git
checkCommandExists npm

# Fetch ${release_version} from pom.xml
exportProjectVersions

kylin_commit_sha1=`git rev-parse HEAD`
if [[ "${current_branch}" = "" ]]; then
    current_branch=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p')
fi
echo "${kylin_commit_sha1}@${current_branch}" > build/commit_SHA1
if [ -z "$ASF_USERNAME" ]; then
    ASF_USERNAME="SOMEONE"
fi
echo "Build by ${ASF_USERNAME} at" `date "+%Y-%m-%d %H:%M:%S"` >> build/commit_SHA1
echo "OS  : `uname -a`" >> build/commit_SHA1

KYLIN_VERSION_NAME="Apache Kylin ${release_version}"

echo "${KYLIN_VERSION_NAME}" > build/VERSION
echo "VERSION file content:" ${KYLIN_VERSION_NAME}

echo "BUILD STAGE 2 - Build binaries..."
bash build/release/build.sh $@             || { exit 1; }

if [[ "${WITH_SPARK}" = "1" ]]; then
    echo "BUILD STAGE 3 - Prepare spark..."
    bash -x build/release/download-spark.sh      || { exit 1; }
else
    rm -rf build/spark
fi

if [[ "${WITH_THIRDPARTY}" = "1" ]]; then
    echo "BUILD STAGE 4 - Prepare influxdb..."
    bash build/release/download-influxdb.sh      || { exit 1; }

    echo "BUILD STAGE 5 - Prepare grafana..."
    bash build/release/download-grafana.sh      || { exit 1; }

    echo "BUILD STAGE 6 - Prepare postgresql..."
    bash build/release/download-postgresql.sh      || { exit 1; }
else
    echo "BUILD STAGE 4-6 is skipped ..."
    rm -rf build/influxdb
    rm -rf build/grafana
    rm -rf build/postgresql
fi

echo "BUILD STAGE 7 - Prepare and compress package..."
bash build/release/prepare.sh                || { exit 1; }
bash build/release/compress.sh               || { exit 1; }

echo "BUILD STAGE 8 - Clean up..."

echo "BUILD FINISHED!"
