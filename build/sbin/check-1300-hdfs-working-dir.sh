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

#title=Checking Permission of HDFS Working Dir

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source ${KYLIN_HOME}/sbin/find-working-dir.sh
source ${KYLIN_HOME}/sbin/init-kerberos.sh

## init Kerberos if needed
initKerberosIfNeeded

echo "Checking HDFS working dir..."

RANDNAME=chkenv__${RANDOM}
TEST_FILE=${WORKING_DIR}/${RANDNAME}

# test local hdfs
## in read-write separation mode this is write cluster
hadoop ${hadoop_conf_param} fs -test -d ${WORKING_DIR} || quit "ERROR: Please check the correctness of hadoop configuration file under ${kylin_hadoop_conf_dir}. At the same time, please make sure the working directory '${WORKING_DIR}' exists and grant access permission to current user."

# test if kylin user (current user) has write permission to working directory
touch ./${RANDNAME}
hadoop ${hadoop_conf_param} fs -put -f ./${RANDNAME} ${TEST_FILE} || quit "ERROR: Have no permission to create/modify file in working directory '${WORKING_DIR}'. Please grant permission to current user."

rm -f ./${RANDNAME}
if [ -n "$TDH_CLIENT_HOME" ]
then
  hadoop ${hadoop_conf_param} fs -rm ${TEST_FILE}
else
  hadoop ${hadoop_conf_param} fs -rm -skipTrash ${TEST_FILE}
fi
