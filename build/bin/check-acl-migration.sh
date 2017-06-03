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

set -o pipefail  # trace ERR through pipes
set -o errtrace  # trace ERR through 'time command' and other functions
function error() {
   SCRIPT="$0"           # script name
   LASTLINE="$1"         # line of error occurrence
   LASTERR="$2"          # error code
   echo "ERROR exit from ${SCRIPT} : line ${LASTLINE} with exit code ${LASTERR}"
   exit 1
}
trap 'error ${LINENO} ${?}' ERR


#check kylin home
if [ -z "$KYLIN_HOME" ]
then
    echo 'Please make sure KYLIN_HOME has been set'
    exit 1
else
    echo "KYLIN_HOME is set to ${KYLIN_HOME}"
fi

echo "Start to check whether we need to migrate acl tables"
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.AclTableMigrationCLI CHECK