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
version=`cat ${KYLIN_HOME}/VERSION | awk '{print $3}'`
${KYLIN_HOME}/sbin/rotate-logs.sh $@

if [ "$1" == "-v" ]; then
    shift
fi

KYLIN_ENV_CHANNEL=`$KYLIN_HOME/bin/get-properties.sh kylin.env.channel`
SPARK_SCHEDULER_MODE=`$KYLIN_HOME/bin/get-properties.sh kylin.query.engine.spark-scheduler-mode`
MAX_CONCURRENT_JOBS=`$KYLIN_HOME/bin/get-properties.sh kylin.job.max-concurrent-jobs`

if [[ $(hadoop version 2>/dev/null) == *"mapr"* ]]; then
    MAPR_AUTHENTICATION="-Djava.security.auth.login.config=${MAPR_HOME}/conf/mapr.login.conf"
fi

if [ "${SPARK_SCHEDULER_MODE}" == "" ] || [[ "${SPARK_SCHEDULER_MODE}" != "FAIR" && "${SPARK_SCHEDULER_MODE}" != "SJF" ]]; then
  SPARK_SCHEDULER_MODE="FAIR"
fi

function prepareEnv {
    export KYLIN_CONFIG_FILE="${KYLIN_HOME}/conf/kylin.properties"
    export SPARK_HOME=${KYLIN_HOME}/spark

    verbose "KYLIN_HOME is:${KYLIN_HOME}"
    verbose "KYLIN_CONFIG_FILE is:${KYLIN_CONFIG_FILE}"
    verbose "SPARK_HOME is:${SPARK_HOME}"

    retrieveDependency

    mkdir -p ${KYLIN_HOME}/logs
    source ${KYLIN_HOME}/sbin/do-check-and-prepare-spark.sh

    # init kerberos
    source ${KYLIN_HOME}/sbin/init-kerberos.sh
    prepareKerberosOpts
    initKerberosIfNeeded
}

function retrieveDependency() {
    # get kylin_hadoop_conf_dir
    if [[ -z ${kylin_hadoop_conf_dir} ]]; then
       source ${KYLIN_HOME}/sbin/prepare-hadoop-conf-dir.sh
    fi

    #retrive $KYLIN_EXTRA_START_OPTS
    source ${KYLIN_HOME}/sbin/setenv.sh
    export KYLIN_EXTRA_START_OPTS=`echo ${KYLIN_EXTRA_START_OPTS}|sed  "s/-XX:+PrintFlagsFinal//g"`
}

function checkRestPort() {
    used=`netstat -tpln | grep "\<$port\>" | awk '{print $7}' | sed "s/\// /g"`
    if [ ! -z "$used" ]; then
        echo "<$used> already listen on $port"
        exit -1
    fi
}

function checkZookeeperRole {
    source ${KYLIN_HOME}/sbin/check-2000-zookeeper-role.sh
}

function checkSparkDir() {
    source ${KYLIN_HOME}/sbin/check-1600-spark-dir.sh
}

function checkHiveDirAcl() {
  source ${KYLIN_HOME}/sbin/check-2100-hive-acl.sh
}

function checkIfStopUserSameAsStartUser() {
    startUser=`ps -p $1 -o user=`
    currentUser=`whoami`

    if [ ${startUser} != ${currentUser} ]; then
        echo `setColor 33 "Warning: You started Kyligence Enterprise as user [${startUser}], please stop the instance as the same user."`
    fi
}

function quit {
    echo "$@"
    if [[ -n "${QUIT_MESSAGE_LOG}" ]]; then
        echo `setColor 31 "$@"` >> ${QUIT_MESSAGE_LOG}
    fi
    exit 1
}


function prepareFairScheduler() {
    cat > ${KYLIN_HOME}/conf/fairscheduler.xml <<EOL
<?xml version="1.0"?>

<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

<allocations>
  <pool name="query_pushdown">
    <schedulingMode>${SPARK_SCHEDULER_MODE}</schedulingMode>
    <weight>1</weight>
    <minShare>1</minShare>
  </pool>
  <pool name="extreme_heavy_tasks">
    <schedulingMode>${SPARK_SCHEDULER_MODE}</schedulingMode>
    <weight>3</weight>
    <minShare>1</minShare>
  </pool>
  <pool name="heavy_tasks">
    <schedulingMode>${SPARK_SCHEDULER_MODE}</schedulingMode>
    <weight>5</weight>
    <minShare>1</minShare>
  </pool>
  <pool name="lightweight_tasks">
    <schedulingMode>${SPARK_SCHEDULER_MODE}</schedulingMode>
    <weight>10</weight>
    <minShare>1</minShare>
  </pool>
  <pool name="vip_tasks">
    <schedulingMode>${SPARK_SCHEDULER_MODE}</schedulingMode>
    <weight>15</weight>
    <minShare>1</minShare>
  </pool>
</allocations>

EOL
}

function runTool() {
    runToolInternal "$@"
    exit $?
}

function runToolInternal() {
    prepareEnv
    if [[ -f ${KYLIN_HOME}/conf/kylin-tools-log4j.xml ]]; then
        kylin_tools_log4j="file:${KYLIN_HOME}/conf/kylin-tools-log4j.xml"
    else
        kylin_tools_log4j="file:${KYLIN_HOME}/tool/conf/kylin-tools-log4j.xml"
    fi
    java -Xms${JAVA_VM_TOOL_XMS} -Xmx${JAVA_VM_TOOL_XMX} ${KYLIN_KERBEROS_OPTS} ${MAPR_AUTHENTICATION} -Dfile.encoding=UTF-8 -Dlog4j.configurationFile=${kylin_tools_log4j} -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} -Dhdp.version=current -cp "${kylin_hadoop_conf_dir}:${KYLIN_HOME}/conf/:${KYLIN_HOME}/lib/ext/*:${KYLIN_HOME}/server/jars/*:${SPARK_HOME}/jars/*" "$@"
}

function killChildProcess {
    if [ -f "${KYLIN_HOME}/child_process" ]
    then
        count=0
        while childPid='' read -r line || [[ -n "$line" ]]; do
            # only kill orphan processes and spark-submit processes
            ((count+=1))
            pid=$( cut -d ',' -f 1 <<< "$line" )
            jobId=$( cut -d ',' -f 2 <<< "$line" )
            rm -r ${KYLIN_HOME}/tmp/$jobId
            parentId=`ps -o ppid=, "$pid"`
            for i in {1..5}
            do
              if ps -p $pid > /dev/null
              then
                if [ "$parentId" -eq 1 ] && ps aux | grep $pid | grep spark-submit > /dev/null
                then
                    verbose "Killing child process $pid"
                    bash ${KYLIN_HOME}/sbin/kill-process-tree.sh $pid
                else
                    sleep 1
                fi
                continue
              fi
              break
            done
            if [[ $count -ge ${MAX_CONCURRENT_JOBS} ]]
            then
                break
            fi
        done < "${KYLIN_HOME}/child_process"
        rm -f ${KYLIN_HOME}/child_process
    fi
}

function clearRedundantProcess {
    if [ -f "${KYLIN_HOME}/pid" ]
    then
        pidKeep=0
        pidRedundant=0
        for pid in `cat ${KYLIN_HOME}/pid`
        do
            pidActive=`ps -ef | grep $pid | grep ${KYLIN_HOME} | wc -l`
            if [ "$pidActive" -eq 1 ]
            then
                if [ "$pidKeep" -eq 0 ]
                then
                    pidKeep=$pid
                else
                    echo "Redundant Kyligence Enterprise process $pid to running process $pidKeep, stop it."
                    bash ${KYLIN_HOME}/sbin/kill-process-tree.sh $pid
                    ((pidRedundant+=1))
                fi
            fi
        done
        if [ "$pidKeep" -ne 0 ]
        then
            echo $pidKeep > ${KYLIN_HOME}/pid
        else
            rm ${KYLIN_HOME}/pid
        fi
        if [ "$pidRedundant" -ne 0 ]
        then
            quit "Kyligence Enterprise is redundant, start canceled."
        fi
    fi
}

function clearCrontab() {
    if ! isCrontabUseable;then
        return 1
    fi
    logrotateDir=${KYLIN_HOME}/logrotate
    if [ -f "${logrotateDir}/cron" ]; then
        rm -f ${logrotateDir}/cron
    fi
    touch ${logrotateDir}/cron
    crontab -l | while read line
    do
        if [[ "$line" == *${logrotateDir}/ke* ]];then
            continue
        fi
        echo "$line" >> ${logrotateDir}/cron
    done
    crontab ${logrotateDir}/cron
}

function isCrontabUseable() {
    crontab -l >/dev/null 2>&1 || (echo "This user don't have permission to run crontab." && return 1)
}

function setLogRotate() {
    auto_log_rotate_enabled=`${dir}/get-properties.sh kylin.env.log-rotate-enabled`
    if [ -d "/etc/logrotate.d" -a ${auto_log_rotate_enabled} == "true" ] && isCrontabUseable; then
        ${dir}/../sbin/log-rotate-cron.sh
    else
        ${dir}/../sbin/rotate-logs.sh
    fi
}

function startKE(){
    setLogRotate
    clearRedundantProcess

    if [ -f "${KYLIN_HOME}/pid" ]; then
        PID=`cat ${KYLIN_HOME}/pid`
        if ps -p $PID > /dev/null; then
          quit "Kylin is running, stop it first, PID is $PID"
        fi
    fi

    ${KYLIN_HOME}/bin/check-env.sh "if-not-yet" || exit 1

    START_TIME=$(date "+%Y-%m-%d %H:%M:%S")

    recordKylinStartOrStop "start" "${START_TIME}"

    killChildProcess

    prepareEnv

    cd ${KYLIN_HOME}/server
    source ${KYLIN_HOME}/sbin/load-zookeeper-config.sh
    fetchFIZkInfo

    prepareFairScheduler

    port=`$KYLIN_HOME/bin/get-properties.sh server.port`
    if [[ -f ${KYLIN_HOME}/bin/check-env-bypass ]]; then
        checkRestPort
        checkZookeeperRole
    fi

    if [[ ${KYLIN_ENV_CHANNEL} == "on-premises" || -z ${KYLIN_ENV_CHANNEL} ]]; then
        checkSparkDir
    fi

    checkHiveDirAcl

    runToolInternal org.apache.kylin.tool.upgrade.AddInstanceColumnCLI

    runToolInternal org.apache.kylin.tool.security.AdminUserInitCLI
    if [[ $? == 1 ]]; then
      quit "Create Admin user failed, for more details please refer to \"\$KYLIN_HOME/logs/shell.stderr\"."
    fi

    if [[ -f ${KYLIN_HOME}/conf/kylin-server-log4j.xml ]]; then
        kylin_server_log4j="file:${KYLIN_HOME}/conf/kylin-server-log4j.xml"
    else
        kylin_server_log4j="file:${KYLIN_HOME}/server/conf/kylin-server-log4j.xml"
    fi

    TIME_ZONE=`${KYLIN_HOME}/bin/get-properties.sh kylin.web.timezone`
    if [[ -n ${TIME_ZONE} ]]; then
        TIME_ZONE="-Duser.timezone=${TIME_ZONE}"
    fi

    SERVER_MODE=`${KYLIN_HOME}/bin/get-properties.sh kylin.server.mode`

    nohup java ${KYLIN_KERBEROS_OPTS} ${KYLIN_EXTRA_START_OPTS} ${TIME_ZONE} -Dfile.encoding=UTF-8 -Dlogging.path=${KYLIN_HOME}/logs -Dspring.profiles.active=prod -Dlogging.config=${kylin_server_log4j} -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} -Dhdp.version=current -Dloader.path="${kylin_hadoop_conf_dir},${KYLIN_HOME}/conf,${KYLIN_HOME}/lib/ext,${KYLIN_HOME}/server/jars,${SPARK_HOME}/jars" -XX:OnOutOfMemoryError="sh ${KYLIN_HOME}/bin/guardian.sh kill"  -jar newten.jar --kylin.server.mode=${SERVER_MODE} >> ${KYLIN_HOME}/logs/kylin.out 2>&1 < /dev/null & echo $! >> ${KYLIN_HOME}/pid &
    sleep 3
    clearRedundantProcess

    PID=`cat ${KYLIN_HOME}/pid`
    CUR_DATE=$(date "+%Y-%m-%d %H:%M:%S")
    echo $CUR_DATE" new KE process pid is "$PID >> ${KYLIN_HOME}/logs/kylin.log

    sh ${KYLIN_HOME}/bin/guardian.sh start

    echo "Kyligence Enterprise is starting. It may take a while. For status, please visit http://`hostname`:$port/kylin/index.html."
    echo "You may also check status via: PID:`cat ${KYLIN_HOME}/pid`, or Log: ${KYLIN_HOME}/logs/kylin.log."
    recordKylinStartOrStop "start success" "${START_TIME}"
}

function stopKE(){
    sh ${KYLIN_HOME}/bin/guardian.sh stop
    clearCrontab

    STOP_TIME=$(date "+%Y-%m-%d %H:%M:%S")
    if [ -f "${KYLIN_HOME}/pid" ]; then
        PID=`cat ${KYLIN_HOME}/pid`
        if ps -p $PID > /dev/null; then

           checkIfStopUserSameAsStartUser $PID

           echo `date '+%Y-%m-%d %H:%M:%S '`"Stopping Kylin: $PID"
           kill $PID
           for i in {1..10}; do
              sleep 3
              if ps -p $PID -f | grep kylin > /dev/null; then
                 if [ "$i" == "10" ]; then
                    echo `date '+%Y-%m-%d %H:%M:%S '`"Killing Kylin: $PID"
                    kill -9 $PID
                 fi
                 continue
              fi
              break
           done
           rm ${KYLIN_HOME}/pid

           killChildProcess
           recordKylinStartOrStop "stop" "${STOP_TIME}"
           return 0
        else
           return 1
        fi

    else
        return 1
    fi
}

function recordKylinStartOrStop() {
    currentIp=$(ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1' | head -n 1)
    serverPort=`$KYLIN_HOME/bin/get-properties.sh server.port`
    echo `date '+%Y-%m-%d %H:%M:%S '`"INFO : [Operation: $1] user:`whoami`, start time:$2, ip and port:${currentIp}:${serverPort}" >> ${KYLIN_HOME}/logs/security.log
}

if [[ "$1" == org.apache.kylin.* ]]; then
    runTool "$@"
# start command
elif [ "$1" == "start" ]; then
    echo "Starting Kyligence Enterprise..."
    startKE
# stop command
elif [ "$1" == "stop" ]; then
    echo `date '+%Y-%m-%d %H:%M:%S '`"Stopping Kyligence Enterprise..."
    stopKE
    if [[ $? == 0 ]]; then
        exit 0
    else
        quit "Kyligence Enterprise is not running"
    fi
# restart command
elif [ "$1" == "restart" ]; then
    echo "Restarting Kyligence Enterprise..."
    echo "--> Stopping Kyligence Enterprise first if it's running..."
    stopKE
    if [[ $? != 0 ]]; then
        echo "    Kyligence Enterprise is not running, now start it"
    fi
    echo "--> Starting Kyligence Enterprise..."
    startKE
else
    quit "Usage: 'kylin.sh [-v] start' or 'kylin.sh [-v] stop' or 'kylin.sh [-v] restart'"
fi
