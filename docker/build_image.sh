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

echo "start build kylin image base on current source code"

rm -rf ./kylin
mkdir -p ./kylin

echo "start copy kylin source code"

for file in `ls ../../kylin/`
do
    if [ docker != $file ]
    then
        cp -r ../../kylin/$file ./kylin/
    fi
done

echo "finish copy kylin source code"

docker build -t apache-kylin-standalone .