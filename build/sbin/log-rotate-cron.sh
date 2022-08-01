#!/bin/bash
# Kyligence Inc. License

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
