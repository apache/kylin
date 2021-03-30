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

if [ ! $1 ]; then
    echo "usage: build-incremental-cube.sh CUBE INTERVAL DELAY"
    exit 1
fi

tomcat_root=${dir}/../tomcat
export tomcat_root

CUBE=$1
INTERVAL=${2:-"3600000"}
DELAY=${3:-"0"}

kylin_rest_address=`hostname -f`":"`grep "<Connector port=" ${tomcat_root}/conf/server.xml |grep protocol=\"HTTP/1.1\" | cut -d '=' -f 2 | cut -d \" -f 2`

CURRENT_TIME_IN_SECOND=`date +%s`
CURRENT_TIME=$((CURRENT_TIME_IN_SECOND * 1000))
END_TIME=$((CURRENT_TIME-DELAY))
END=$((END_TIME - END_TIME%INTERVAL))

ID="$END"
echo "Building for ${CUBE}_${ID}" | tee ${KYLIN_HOME}/logs/build_trace.log
echo "Check the log at ${KYLIN_HOME}/logs/incremental_cube_${CUBE}_${END}.log"
curl -X PUT -H "Authorization: Basic %Auth%" -H "Content-Type: application/json;charset=utf-8" -d "{\"endTime\": ${END}, \"buildType\": \"BUILD\"}" http://${kylin_rest_address}/kylin/api/cubes/${CUBE}/rebuild  > ${KYLIN_HOME}/logs/incremental_cube_${CUBE}_${END}.log 2>&1 &

