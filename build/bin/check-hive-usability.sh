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

source ${KYLIN_HOME:-"$(cd -P -- "$(dirname -- "$0")" && pwd -P)/../"}/bin/header.sh

hive -e "select 1" > /dev/null 2>&1 &
pid=$!

((timeLeft = 60))

while ((timeLeft > 0)); do
    sleep 5
    ps -p $pid > /dev/null || break
    ((timeLeft -= 5))
done

if ps -p $pid > /dev/null
then
    kill $pid
    sleep 5

    # still alive, use kill -9
    if ps -p $pid > /dev/null
    then
        kill -9 $pid
    fi

    quit "ERROR: Check hive's usability failed, please check the status of your cluster"
fi
