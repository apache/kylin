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

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh $@

function addCrontab() {
    logrotateCmd="${cronExpress} /usr/sbin/logrotate -s ${logrotateDir}/status ${logrotateDir}/ke > /dev/null 2>&1"
    crontab -l | while read line
    do
        if [[ "$line" == *${logrotateDir}/ke* ]];then
            continue
        fi
        echo "$line" >> ${logrotateDir}/cron
    done
    echo "$logrotateCmd" >> ${logrotateDir}/cron
    crontab ${logrotateDir}/cron
}

function rmCronConf() {
    if [ -f "${logrotateDir}/cron" ]; then
        rm -f ${logrotateDir}/cron
    fi
}

function creatConf(){
  cat > ${logrotateDir}/ke <<EOL
${ERR_LOG} ${OUT_LOG} ${KYLIN_OUT}  {
size ${file_threshold}M
rotate ${keep_limit}
missingok
copytruncate
nodateext
}
EOL
}

ERR_LOG=${KYLIN_HOME}/logs/shell.stderr
OUT_LOG=${KYLIN_HOME}/logs/shell.stdout
KYLIN_OUT=${KYLIN_HOME}/logs/kylin.out
logrotateDir=${KYLIN_HOME}/logrotate

file_threshold=`${dir}/get-properties.sh kylin.env.max-keep-log-file-threshold-mb`
keep_limit=`${dir}/get-properties.sh kylin.env.max-keep-log-file-number`
cronExpress=`${dir}/get-properties.sh kylin.env.log-rotate-check-cron`

if [ ! -d "$logrotateDir" ]; then
    mkdir $logrotateDir
fi
creatConf
rmCronConf
addCrontab
