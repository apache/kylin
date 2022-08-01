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
exportProjectVersions

sh build/apache_release/prepare-libs.sh || { exit 1; }

#create ext dir
mkdir -p build/ext

mkdir -p build/server
chmod -R 755 build/server

cp src/server/target/kylin-server-${kylin_version}.jar build/server/newten.jar
cp -r src/server/target/jars build/server/

if [[ ${WITH_FRONT} == 0 ]]; then
  echo "Skip frontend packing..."
else
  echo "Start to add js & css..."
  if [ ! -d "kystudio/dist" ]; then
      echo "Failed to generate js files!"
      exit 1
  fi

  cd kystudio
  mkdir -p ../build/server/webapp

  cp -rf ./dist ../build/server/webapp

  echo "End frontend packing..."
fi

