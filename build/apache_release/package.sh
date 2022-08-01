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

source build/apache_release/functions.sh

echo "Packing for KE..."

# Make share commands exist in environment
echo "BUILD STAGE 1 - Checking environment..."
checkCommandExists mvn
checkCommandExists git
checkCommandExists npm

exportProjectVersions

kap_commit_sha1=`git rev-parse HEAD`
echo "${kap_commit_sha1}@KAP" > build/commit_SHA1
if [ -z "$BUILD_SYSTEM" ]; then
    BUILD_SYSTEM="MANUAL"
fi
echo "Build with ${BUILD_SYSTEM} at" `date "+%Y-%m-%d %H:%M:%S"` >> build/commit_SHA1

KYLIN_VERSION_NAME="Kylin 5 ${release_version}"

echo "${KYLIN_VERSION_NAME}" > build/VERSION
echo "VERSION file content:" ${KYLIN_VERSION_NAME}

echo "BUILD STAGE 2 - Build binaries..."
sh build/apache_release/build.sh $@             || { exit 1; }

if [[ "${WITH_SPARK}" = "1" ]]; then
    echo "BUILD STAGE 3 - Prepare spark..."
    sh -x build/apache_release/download-spark.sh      || { exit 1; }
else
    rm -rf build/spark
fi

if [[ "${WITH_THIRDPARTY}" = "1" ]]; then
    #echo "BUILD STAGE 4 - Prepare influxdb..."
    #sh build/apache_release/download-influxdb.sh      || { exit 1; }

    #echo "BUILD STAGE 5 - Prepare grafana..."
    #sh build/apache_release/download-grafana.sh      || { exit 1; }

    echo "BUILD STAGE 6 - Prepare postgresql..."
    sh build/apache_release/download-postgresql.sh      || { exit 1; }
else
    rm -rf build/influxdb
    rm -rf build/grafana
    rm -rf build/postgresql
fi

echo "BUILD STAGE 7 - Prepare and compress package..."
sh build/apache_release/prepare.sh ${MVN_PROFILE} || { exit 1; }
sh build/apache_release/compress.sh               || { exit 1; }

echo "BUILD STAGE 8 - Clean up..."
    
echo "BUILD FINISHED!"
