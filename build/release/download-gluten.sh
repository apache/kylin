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

rm -rf build/gluten

gluten_version=$(sed -n 's:.*<gluten.version>\(.*\)</gluten.version>.*:\1:p'  pom.xml)
# optional
gluten_platform='ubuntu22.04-x86_64'
if [ ! -f "build/gluten-${gluten_version}-${gluten_platform}.tar.gz" ]; then
  echo "no binary file found"
  wget --directory-prefix=build/ https://repository.kyligence.io/repository/open-raw/org/apache/gluten/${gluten_version}-${gluten_platform}/gluten-${gluten_version}-${gluten_platform}.tar.gz
fi

tar -zxf build/gluten-${gluten_version}-${gluten_platform}.tar.gz -C build/
mv build/gluten-${gluten_version}-${gluten_platform} build/gluten
cp build/gluten/libs/libch.so build/spark/
find build/spark/jars/ -name "protobuf-java*" -delete
cp -r build/gluten/jars/spark33/* build/spark/jars/
