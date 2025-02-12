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
source "${KYLIN_HOME}"/sbin/init-customer-env.sh
version=$(cat "${KYLIN_HOME}"/VERSION | awk '{print $3}')
"${KYLIN_HOME}"/sbin/rotate-logs.sh "$@"

export KYLIN_SKIP_CHECK=${KYLIN_SKIP_CHECK:-0}
KYLIN_SKIP_CHECK_MODE=1
export KYLIN_WITH_GLUTEN=${KYLIN_WITH_GLUTEN:-1}

if [ "$1" == "-v" ]; then
    shift
fi

if [[ $(hadoop version 2>/dev/null) == *"mapr"* ]]; then
    MAPR_AUTHENTICATION="-Djava.security.auth.login.config=${MAPR_HOME}/conf/mapr.login.conf"
fi

if [ "${SPARK_SCHEDULER_MODE}" == "" ] || [[ "${SPARK_SCHEDULER_MODE}" != "FAIR" && "${SPARK_SCHEDULER_MODE}" != "SJF" ]]; then
  SPARK_SCHEDULER_MODE="FAIR"
fi

function prepareEnv() {
    # avoid re-entering
    if [[ -n $SKIP_PRE_ENV ]]; then
        return
    fi

    SKIP_PRE_ENV=1

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

    # init gluten libch.so
    if [[ -f ${KYLIN_HOME}/server/libch.so ]]; then
      export LD_PRELOAD=${KYLIN_HOME}/server/libch.so
      verbose "LD_PRELOAD= is:${LD_PRELOAD}"
    fi

    # check if start with gluten
    if [[ ${KYLIN_WITH_GLUTEN} -eq 0 ]]; then
      verbose "start without gluten"
      if [[ -f ${KYLIN_HOME}/lib/ext/gluten.jar ]]; then
        mv "${KYLIN_HOME}"/lib/ext/gluten.jar "${KYLIN_HOME}"/lib/ext/gluten.jar.bak
      fi

      if [[ -f ${KYLIN_HOME}/spark/jars/gluten.jar ]]; then
        mv "${KYLIN_HOME}"/spark/jars/gluten.jar "${KYLIN_HOME}"/spark/jars/gluten.jar.bak
      fi

      gluten_shims_jar=$(find "${KYLIN_HOME}"/server/jars -name "spark-sql-columnar-shims-kyspark-*.jar")
      if [[  -f "$gluten_shims_jar" ]]; then
        mv "$gluten_shims_jar" "$gluten_shims_jar".bak
      fi

      config_without_gluten=$(grep -vE "^#" "${KYLIN_HOME}/conf/kylin.properties.withoutGluten" | grep -E "^kylin.*")
      while read -r line
      do
        sed -i "/${line}/d" "${KYLIN_CONFIG_FILE}"
      done <<< "$config_without_gluten"
      echo "$config_without_gluten" >> "${KYLIN_CONFIG_FILE}"
    else
      verbose "start with gluten"
      if [[ -f ${KYLIN_HOME}/lib/ext/gluten.jar.bak ]]; then
        mv "${KYLIN_HOME}"/lib/ext/gluten.jar.bak "${KYLIN_HOME}"/lib/ext/gluten.jar
      fi

      if [[ -f ${KYLIN_HOME}/spark/jars/gluten.jar.bak ]]; then
        mv "${KYLIN_HOME}"/spark/jars/gluten.jar.bak "${KYLIN_HOME}"/spark/jars/gluten.jar
      fi

      gluten_shims_jar=$(find "${KYLIN_HOME}"/server/jars -name "spark-sql-columnar-shims-kyspark-*.jar.bak")
      if [[  -f "$gluten_shims_jar" ]]; then
        mv "$gluten_shims_jar" "${gluten_shims_jar%.bak}"
      fi

      config_without_gluten=$(grep -vE "^#" "${KYLIN_HOME}/conf/kylin.properties.withoutGluten" | grep -E "^kylin.*")
      while read -r line
      do
        sed -i "/${line}/d" "${KYLIN_CONFIG_FILE}"
      done <<< "$config_without_gluten"
    fi

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
    SERVER_PORT=${SERVER_PORT:-$("$KYLIN_HOME"/bin/get-properties.sh server.port)}
    local used=$(netstat -tpln | grep "\<$SERVER_PORT\>" | awk '{print $7}' | sed "s/\// /g")
    if [ ! -z "$used" ]; then
        echo "<$used> already listen on $SERVER_PORT"
        exit 1
    fi
}

function skipCheckOrNot() {
    if [[ $KYLIN_SKIP_CHECK -ge $1 ]]; then
        echo "true"
    else
      echo ""
    fi
}

function checkZookeeperRole() {

    KYLIN_ZOOKEEPER_ENABLED=$("$KYLIN_HOME"/bin/get-properties.sh kylin.env.zookeeper.enabled)
    if [[ $KYLIN_ZOOKEEPER_ENABLED == "true" ]]; then
        source ${KYLIN_HOME}/sbin/check-2000-zookeeper-role.sh
    fi
}

function checkSparkDir() {
    if [[ $(skipCheckOrNot $KYLIN_SKIP_CHECK_MODE) ]]; then
        return 0
    fi

    if [[ ${KYLIN_ENV_CHANNEL} == "on-premises" || -z ${KYLIN_ENV_CHANNEL} ]]; then
      verboseLog "checking spark dir"
      source "${KYLIN_HOME}"/sbin/check-1600-spark-dir.sh
    fi
}

function checkIfStopUserSameAsStartUser() {
    if [[ `skipCheckOrNot $KYLIN_SKIP_CHECK_MODE` ]]; then
        return 0
    fi
    verboseLog "checking stop user"

    startUser=$(ps -p "$1" -o user=)
    currentUser=$(whoami)

    if [[ ${startUser} != "${currentUser}" ]]; then
        echo $(setColor 33 "Warning: You started Kylin as user [${startUser}], please stop the instance as the same user.")
    fi
}

function quit {
    echo "$@"
    if [[ -n "${QUIT_MESSAGE_LOG}" ]]; then
        echo $(setColor 31 "$@") >> "${QUIT_MESSAGE_LOG}"
    fi
    exit 1
}


function prepareFairScheduler() {
    local spark_scheduler_mode=$("$KYLIN_HOME"/bin/get-properties.sh kylin.query.engine.spark-scheduler-mode)
    if [ "${spark_scheduler_mode}" == "" ] || [[ "${spark_scheduler_mode}" != "FAIR" && "${spark_scheduler_mode}" != "SJF" ]]; then
      spark_scheduler_mode="FAIR"
    fi

    cat > "${KYLIN_HOME}"/conf/fairscheduler.xml <<EOL
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
    java -Xms${JAVA_VM_TOOL_XMS} -Xmx${JAVA_VM_TOOL_XMX} ${KYLIN_KERBEROS_OPTS} -Dfile.encoding=UTF-8 -Dlog4j.configurationFile=${kylin_tools_log4j} -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} -Dhdp.version=current -cp "${kylin_hadoop_conf_dir}:${KYLIN_HOME}/conf/:${KYLIN_HOME}/lib/ext/*:${SPARK_HOME}/jars/*:${KYLIN_HOME}/server/jars/*" "$@"
}

function killChildProcess {
    if [ -f "${KYLIN_HOME}/child_process" ]
    then
        MAX_CONCURRENT_JOBS=$("$KYLIN_HOME"/bin/get-properties.sh kylin.job.max-concurrent-jobs)
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
    if [[ $(skipCheckOrNot $KYLIN_SKIP_CHECK_MODE) ]]; then
        return 0
    fi

    verboseLog "checking redundant process"

    #sleep or not
    if [[ -n $1 ]]; then
      sleep "$1"
    fi

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
                    echo "Redundant Kylin process $pid to running process $pidKeep, stop it."
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
            quit "Kylin is redundant, start canceled."
        fi
    fi
}

function checkKylinMetaList() {
    if [[ $(skipCheckOrNot $KYLIN_SKIP_CHECK_MODE) ]]; then
      return 0
    fi

    verboseLog "checking kylin meta"

    runToolInternal org.apache.kylin.tool.upgrade.UpdateSessionTableColumnLengthCLI

    runToolInternal org.apache.kylin.tool.security.AdminUserInitCLI

}

function checkLog4jConf() {
    if [[ -f ${KYLIN_HOME}/conf/kylin-server-log4j.xml ]]; then
        KYLIN_SERVER_LOG4J="file:${KYLIN_HOME}/conf/kylin-server-log4j.xml"
    else
        KYLIN_SERVER_LOG4J="file:${KYLIN_HOME}/server/conf/kylin-server-log4j.xml"
    fi
}

function checkTimeZone() {
    TIME_ZONE=$("$KYLIN_HOME"/bin/get-properties.sh kylin.web.timezone)
    if [[ -n ${TIME_ZONE} ]]; then
        TIME_ZONE="-Duser.timezone=${TIME_ZONE}"
    fi
}

function checkEnv() {
    "${KYLIN_HOME}"/bin/check-env.sh "if-not-yet" || exit 1
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
        if [[ "$line" == *${logrotateDir}/kylin* ]];then
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

function startKylin(){
    setLogRotate
    clearRedundantProcess

    if [ -f "${KYLIN_HOME}/pid" ]; then
        PID=`cat ${KYLIN_HOME}/pid`
        if ps -p $PID > /dev/null; then
          quit "Kylin is running, stop it first, PID is $PID"
        fi
    fi

    checkEnv

    START_TIME=$(date "+%Y-%m-%d %H:%M:%S")

    recordKylinStartOrStop "start" "${START_TIME}"

    killChildProcess

    prepareEnv

    prepareFairScheduler

    #this is necessary in FI
    source "${KYLIN_HOME}"/sbin/load-zookeeper-config.sh

    if [[ -f ${KYLIN_HOME}/bin/check-env-bypass ]]; then
        checkRestPort
        checkZookeeperRole
    fi

    checkSparkDir

    checkKylinMetaList

    checkLog4jConf

    checkTimeZone

    SERVER_MODE=$("${KYLIN_HOME}"/bin/get-properties.sh kylin.server.mode)

    cd "${KYLIN_HOME}"/server
    nohup java ${KYLIN_KERBEROS_OPTS} ${KYLIN_EXTRA_START_OPTS} ${TIME_ZONE} -Dfile.encoding=UTF-8 -Dlogging.path=${KYLIN_HOME}/logs -Dspring.profiles.active=prod -Dlogging.config=${KYLIN_SERVER_LOG4J} -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} -Dhdp.version=current -Dloader.path="${kylin_hadoop_conf_dir},${KYLIN_HOME}/conf,${KYLIN_HOME}/lib/ext,${KYLIN_HOME}/server/jars,${SPARK_HOME}/jars" -XX:OnOutOfMemoryError="sh ${KYLIN_HOME}/bin/guardian.sh kill"  -jar newten.jar --kylin.server.mode=${SERVER_MODE} >> ${KYLIN_HOME}/logs/kylin.out 2>&1 < /dev/null & echo $! >> ${KYLIN_HOME}/pid &

    clearRedundantProcess 3

    PID=$(cat "${KYLIN_HOME}"/pid)
    CUR_DATE=$(date "+%Y-%m-%d %H:%M:%S")
    echo $CUR_DATE" new Kylin process pid is "$PID >> ${KYLIN_HOME}/logs/kylin.log

    sh "${KYLIN_HOME}"/bin/guardian.sh start

    echo "Kylin is starting. It may take a while. For status, please visit http://$(hostname):$SERVER_PORT/kylin/index.html."
    echo "You may also check status via: PID:$(cat "${KYLIN_HOME}"/pid), or Log: ${KYLIN_HOME}/logs/kylin.log."
    recordKylinStartOrStop "start success" "${START_TIME}"
}

function stopKylin(){
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
    SERVER_PORT=${SERVER_PORT:-$("$KYLIN_HOME"/bin/get-properties.sh server.port)}
    echo $(date '+%Y-%m-%d %H:%M:%S ')"INFO : [Operation: $1] user:$(whoami), start time:$2, ip and port:${currentIp}:${SERVER_PORT}" >> "${KYLIN_HOME}"/logs/security.log
}

if [[ "$1" == org.apache.kylin.* ]]; then
    runTool "$@"
# start command
elif [ "$1" == "start" ]; then
    echo "Starting Kylin..."
    startKylin
# stop command
elif [ "$1" == "stop" ]; then
    echo `date '+%Y-%m-%d %H:%M:%S '`"Stopping Kylin..."
    stopKylin
    if [[ $? == 0 ]]; then
        exit 0
    else
        quit "Kylin is not running"
    fi
# restart command
elif [ "$1" == "restart" ]; then
    echo "Restarting Kylin..."
    echo "--> Stopping Kylin first if it's running..."
    stopKylin
    if [[ $? != 0 ]]; then
        echo "    Kylin is not running, now start it"
    fi
    echo "--> Starting Kylin..."
    startKylin
else
    quit "Usage: 'kylin.sh [-v] start' or 'kylin.sh [-v] stop' or 'kylin.sh [-v] restart'"
fi
