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

## DEFAULT: get ke process status
## return code
## 0 process is running
## 1 process is stopped
## -1 process is crashed

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

PID_FILE=$1

if [[ -z ${PID_FILE} ]]; then
    PID_FILE=${KYLIN_HOME}/pid
fi

if [[ -f ${PID_FILE} ]]; then
    PID=`cat ${PID_FILE}`
    PROCESS_COUNT=`ps -p ${PID} | grep "${PID}" | wc -l`

    if [[ ${PROCESS_COUNT} -lt 1 ]]; then
        echo "-1"
    else
        echo "0"
    fi
else
    echo "1"
fi