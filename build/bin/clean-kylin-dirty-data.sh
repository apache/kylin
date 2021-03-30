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

source ${KYLIN_HOME:-"$(cd -P -- "$(dirname -- "$0")" && pwd -P)/../"}/bin/set-kylin-home.sh $@

today=`date +"%Y-%m-%d"`
before7day=`date -d "-7 day" +"%Y-%m-%d"`

echo "*******************start clean metastore***************************"
${KYLIN_HOME}/bin/metastore.sh clean --delete true --jobThreshold 91 >> ${KYLIN_HOME}/logs/clean-kylin-dirty-data.log.$today 2>&1

echo "*******************start clean storage***************************"
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.StorageCleanupJob --delete true >> ${KYLIN_HOME}/logs/clean-kylin-dirty-data.log.$today 2>&1
#clean before 7 day log
before7dayfile=${KYLIN_HOME}/logs/clean-kylin-dirty-data.log.$before7day
if [ -f "$before7dayfile" ]; then
    rm $before7dayfile
fi
