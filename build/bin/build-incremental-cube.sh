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
if [ -z "$KYLIN_HOME" ]; then
	export KYLIN_HOME=${dir}/../
fi
echo KYLIN_HOME is set to $KYLIN_HOME

if [ ! $1 ]; then
	echo "usage: build-incremental-cube.sh CUBE INTERVAL DELAY"
	exit 1
fi

CUBE=$1
INTERVAL=${2:-"3600000"}
DELAY=${3:-"0"}
SERVER="localhost"
PORT="7070"

CURRENT_TIME_IN_SECOND=`date +%s`
CURRENT_TIME=$((CURRENT_TIME_IN_SECOND * 1000))
END_TIME=$((CURRENT_TIME-DELAY))
END=$((END_TIME - END_TIME%INTERVAL))

ID="$END"
echo "Building for ${CUBE}_${ID}" | tee ${KYLIN_HOME}/logs/build_trace.log
echo "Check the log at ${KYLIN_HOME}/logs/incremental_cube_${CUBE}_${END}.log"
curl -X PUT --user ADMIN:KYLIN -H "Content-Type: application/json;charset=utf-8" -d "{\"endTime\": ${END}, \"buildType\": \"BUILD\"}" http://${SERVER}:${PORT}/kylin/api/cubes/${CUBE}/rebuild  > ${KYLIN_HOME}/logs/incremental_cube_${CUBE}_${END}.log 2>&1 &
