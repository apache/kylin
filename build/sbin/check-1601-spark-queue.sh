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

#title=Checking Spark Queue

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
source ${KYLIN_HOME}/sbin/init-kerberos.sh

## init Kerberos if needed
initKerberosIfNeeded

echo "Checking Spark Queue..."

function checkQueueSettings() {
  # get queue settings
  kylin_storage_queue=$($KYLIN_HOME/bin/get-properties.sh kylin.storage.columnar.spark-conf.spark.yarn.queue)
  kylin_engine_queue=$($KYLIN_HOME/bin/get-properties.sh kylin.engine.spark-conf.spark.yarn.queue)

  # set queue=default if the queue is empty
  [[ -z "${kylin_storage_queue}" ]] && kylin_storage_queue=default
  [[ -z "${kylin_engine_queue}" ]] && kylin_engine_queue=default

  # get current username
  user_name=$(mapred queue -showacls | awk 'NR==1 {print $6}')

  # list all the queue
  queue_list_str=$(mapred queue -showacls | awk 'NR>4  {print $1}')
  queue_arr=($queue_list_str)

  # flag variable to indicate whether the queue is valid or not
  storage_vaild=false
  engine_vaild=false

  for queue in "${queue_arr[@]}"; do
    # In case of 'xxx.xxx.queuename'.
    # If the queue is 'root.user.queuename', we need to get 'queuename' instead of 'root.user.queuename'.
    queue=${queue##*.}
    if [ $queue = $kylin_storage_queue ]; then
      storage_vaild=true
    fi
    if [ $queue = $kylin_engine_queue ]; then
      engine_vaild=true
    fi
  done

  if $storage_vaild && $engine_vaild; then
    echo "kylin_storage_queue: '$kylin_storage_queue' and kylin_engine_queue: '$kylin_engine_queue' exist."
  elif $storage_vaild; then
    quit "ERROR: Checking Spark Queue failed, kylin_engine_queue: '$kylin_engine_queue' does not exist."
  elif $engine_vaild; then
    quit "ERROR: Checking Spark Queue failed, kylin_storage_queue: '$kylin_storage_queue' does not exist."
  else
    quit "ERROR: Checking Spark Queue failed, kylin_storage_queue: '$kylin_storage_queue' and kylin_engine_queue: '$kylin_engine_queue' does not exist."
  fi

  # check SUBMIT permission
  submit_reg='.*SUBMIT_APPLICATIONS.*'
  storage_submit_info=$(mapred queue -showacls | awk '$1=="'$kylin_storage_queue'" {print $2}')
  engine_submit_info=$(mapred queue -showacls | awk '$1=="'$kylin_engine_queue'" {print $2}')

  if [[ "$storage_submit_info" =~ $submit_reg ]] && [[ "$engine_submit_info" =~ $submit_reg ]]; then
    echo "'$user_name' can submit to '$kylin_storage_queue' and '$kylin_engine_queue'"
  elif [[ "$storage_submit_info" =~ $submit_reg ]]; then
    quit "ERROR: Checking Spark Queue failed, '$user_name' can not submit task to kylin_engine_queue: '$kylin_engine_queue'"
  elif [[ "$engine_submit_info" =~ $submit_reg ]]; then
    quit "ERROR: Checking Spark Queue failed, '$user_name' can not submit task to kylin_storage_queue: '$kylin_storage_queue'"
  else
    quit "ERROR: Checking Spark Queue failed, '$user_name' can not submit task to kylin_storage_queue: '$kylin_storage_queue' and kylin_engine_queue: '$kylin_engine_queue'"
  fi

  # check the queue is running or not
  running_str='running'
  storage_running_info=$(mapred queue -info $kylin_storage_queue | awk '$2=="State" {print $4}')
  engine_running_info=$(mapred queue -info $kylin_engine_queue | awk '$2=="State" {print $4}')

  if [ $storage_running_info = $running_str ] && [ $engine_running_info = $running_str ]; then
    echo "Queue: kylin_storage_queue: '$kylin_storage_queue' and Queue: kylin_engine_queue: '$kylin_engine_queue' are running"
  elif [ $storage_running_info = $running_str ]; then
    quit "ERROR: Checking Spark Queue failed, Queue: kylin_engine_queue: '$kylin_engine_queue' is not running"
  elif [ $engine_running_info = $running_str ]; then
    quit "ERROR: Checking Spark Queue failed, Queue: kylin_storage_queue: '$kylin_storage_queue' is not running"
  else
    quit "ERROR: Checking Spark Queue failed, Queue: kylin_storage_queue: '$kylin_storage_queue' and Queue: kylin_engine_queue: '$kylin_engine_queue' are not running"
  fi

}

if [[ $(is_kap_kerberos_enabled) == 1 ]]; then
  kylin_kerberos_platform=$($KYLIN_HOME/bin/get-properties.sh kylin.kerberos.platform)
  if [ $kylin_kerberos_platform = FI ]; then
    checkQueueSettings
  elif [ $kylin_kerberos_platform = Standard ]; then
    echo "Not implemented yet, skip checking queue setting."
    exit 3
  else
    echo "Skip checking queue setting."
    exit 3
  fi
else
  echo "Kerberos is not enabled, skip checking queue setting."
  exit 3
fi
echo "Checking Spark Queue succeed"
