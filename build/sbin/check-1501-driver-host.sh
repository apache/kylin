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
#title=Checking Spark Driver Host

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
source ${KYLIN_HOME}/sbin/init-kerberos.sh

## init Kerberos if needed
initKerberosIfNeeded

echo "Checking Spark Driver Host..."

# for query
kylin_storage_host=$($KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.spark-conf.spark.driver.host)

# for build
kylin_engine_deploymode=$($KYLIN_HOME/bin/get-properties.sh kylin.engine.spark-conf.spark.submit.deployMode)
kylin_engine_host=$($KYLIN_HOME/bin/get-properties.sh kylin.engine.spark-conf.spark.driver.host)

#ipv4 or ipv6
ip_reg='^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^([0-9a-fA-F]{1,4}:){6}((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^::([0-9a-fA-F]{1,4}:){0,4}((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^([0-9a-fA-F]{1,4}:):([0-9a-fA-F]{1,4}:){0,3}((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^([0-9a-fA-F]{1,4}:){2}:([0-9a-fA-F]{1,4}:){0,2}((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^([0-9a-fA-F]{1,4}:){3}:([0-9a-fA-F]{1,4}:){0,1}((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^([0-9a-fA-F]{1,4}:){4}:((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$|^:((:[0-9a-fA-F]{1,4}){1,6}|:)$|^[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,5}|:)$|^([0-9a-fA-F]{1,4}:){2}((:[0-9a-fA-F]{1,4}){1,4}|:)$|^([0-9a-fA-F]{1,4}:){3}((:[0-9a-fA-F]{1,4}){1,3}|:)$|^([0-9a-fA-F]{1,4}:){4}((:[0-9a-fA-F]{1,4}){1,2}|:)$|^([0-9a-fA-F]{1,4}:){5}:([0-9a-fA-F]{1,4})?$|^([0-9a-fA-F]{1,4}:){6}:$'

# 0:succeed 1:ERROR 4:WARN
status=0
log_str="Current kylin_engine_deploymode is '$kylin_engine_deploymode'.\n"

# kylin_storage_host need to be checked every time
if [ -z $kylin_storage_host ]; then
  log_str=$log_str"WARN: 'kylin.storage.columnar.spark-conf.spark.driver.host' is missed, it may cause some problems.\n"
  status=4
else
  if [[ "$kylin_storage_host" =~ $ip_reg ]]; then
    log_str=$log_str"PASS: 'kylin.storage.columnar.spark-conf.spark.driver.host:' '$kylin_storage_host' is valid, checking passed.\n"
  else
    log_str=$log_str"ERROR: 'kylin.storage.columnar.spark-conf.spark.driver.host:' '$kylin_storage_host' is not a valid ip address, checking failed.\n"
    status=1
  fi
fi

# kylin_engine_host need to be a valid ip address when kylin_engine_deploymode is "client"
# and it should not be set when kylin_engine_deploymode is "cluster"
# cluster
if [ $kylin_engine_deploymode = 'cluster' ]; then
  if [ -z $kylin_engine_host ]; then
    log_str=$log_str"PASS: 'kylin.engine.spark-conf.spark.driver.host' should not be set when kylin_engine_deploymode is 'cluster', and it is not set, checking passed"
  else
    log_str=$log_str"ERROR: 'kylin.engine.spark-conf.spark.driver.host'should not be set when kylin_engine_deploymode is 'cluster', but it is set as '$kylin_engine_host', checking failed"
    status=1
  fi
# client
elif [ $kylin_engine_deploymode = 'client' ]; then
  if [[ "$kylin_engine_host" =~ $ip_reg ]]; then
    log_str=$log_str"PASS: 'kylin.engine.spark-conf.spark.driver.host:' '$kylin_engine_host' is valid, checking passed."
  elif [ -z $kylin_engine_host ]; then
    log_str=$log_str"WARN: 'kylin.engine.spark-conf.spark.driver.host' is missed, it may cause some problems."
    if [ $status = 0 ]; then
      status=4
    fi
  else
    log_str=$log_str"ERROR: 'kylin.engine.spark-conf.spark.driver.host:' '$kylin_engine_host' is not a valid ip address, checking failed."
    status=1
  fi
else
  log_str=$log_str"WARN: 'kylin.engine.spark-conf.spark.submit.deployMode' should be 'client' or 'cluster', but it is set as '$kylin_engine_deploymode', please check the 'kylin.properties'."
  if [ $status = 0 ]; then
    status=4
  fi
fi

echo -e $log_str

if [ $status = 0 ]; then
  echo "Checking Spark Driver Host succeed"
  exit 0
elif [ $status = 1 ]; then
  quit "ERROR: $log_str"
elif [ $status = 4 ]; then
  exit 4
fi
