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

# ============================================================================

base=master

# ============================================================================

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

# ============================================================================

git fetch apache
git checkout apache/$base-hbase1.x
git format-patch -1
git checkout apache/$base-cdh5.7
git format-patch -1

git checkout apache/$base
git checkout -b tmp
git reset apache/$base --hard

git am -3 --ignore-whitespace 0001-KYLIN-1528-Create-a-branch-for-v1.5-with-HBase-1.x-A.patch
mvn clean compile -DskipTests
git push apache tmp:$base-hbase1.x -f
rm 0001-KYLIN-1528-Create-a-branch-for-v1.5-with-HBase-1.x-A.patch

git am -3 --ignore-whitespace 0001-KYLIN-1672-support-kylin-on-cdh-5.7.patch
mvn clean compile -DskipTests
git push apache tmp:$base-cdh5.7 -f
rm 0001-KYLIN-1672-support-kylin-on-cdh-5.7.patch

# clean up
git checkout master
git reset apache/master --hard
git branch -D tmp
