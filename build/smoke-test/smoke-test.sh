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
# PLEASE BE NOTICED
#
# This tool will test some main functionality of kylin binary package, such as metadata, cubing and query with rest APIs.
# And this tool will use default port(7070) and default metastore name(kylin_metastore) in hdp sandbox. So those resources
# will be overridden in the test.
#
# This tool accepts two parameters, the first is tar package of kylin, the second is target path for decompress, which
# will contain KYLIN_HOME.
#
# Usage: bash smoke-test.sh ../dist/apache-kylin-1.5.0-bin.tar.gz ../dist/apache-kylin-1.5.0-bin
#
# The process of smoke test is based on sample metadata & data.
# 1. run sample.sh to load sample data
# 2. use rest API to build cube
# 3. use rest API to execute some SQL statements, which locates under sql directory
# 4. compare query result with result file under sql directory

PKG_PATH=$1
TARGET_PATH=$2

cd $(dirname ${0})/..
dir=`pwd`
mkdir -p ${TARGET_PATH}

# Setup stage
KYLIN_PID=`cat "${TARGET_PATH}/*kylin*/pid"`
if [ -n "${KYLIN_PID}" ]; then
    if ps -p ${KYLIN_PID} > /dev/null; then
        echo "Kylin is running, will be killed. (pid=${KYILN_PID})"
        kill -9 ${KYLIN_PID}
    fi
fi

rm -rf ${TARGET_PATH}/*kylin*
tar -zxvf $PKG_PATH -C ${TARGET_PATH}

cd ${TARGET_PATH}/*kylin*/
export KYLIN_HOME=`pwd`
cd -

${KYLIN_HOME}/bin/metastore.sh reset

# Test stage
${KYLIN_HOME}/bin/sample.sh

${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.AclTableMigrationCLI MIGRATE

# Enable query push down
cd ${KYLIN_HOME}
sed -i 's/#*\(kylin.query.pushdown.runner-class-name*\)/\1/' conf/kylin.properties
sed -i 's/#*\(kylin.query.pushdown.jdbc.url*\)/\1/' conf/kylin.properties
sed -i 's/#*\(kylin.query.pushdown.jdbc.driver*\)/\1/' conf/kylin.properties
sed -i 's/#*\(kylin.query.pushdown.jdbc.username*\)/\1/' conf/kylin.properties

${KYLIN_HOME}/bin/kylin.sh start

echo "Wait 3 minutes for service start."
sleep 3m

cd $dir/smoke-test
python testBuildCube.py     || { exit 1; }
python testQuery.py         || { exit 1; }
python testDiag.py         || { exit 1; }
cd -

# Tear down stage
${KYLIN_HOME}/bin/metastore.sh clean --delete true
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.StorageCleanupJob --delete true
${KYLIN_HOME}/bin/metastore.sh reset
${KYLIN_HOME}/bin/kylin.sh stop
