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

if [[ -z $KYLIN_HOME ]];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

cd $KYLIN_HOME/grafana

function startGrafana(){
    if [[ -f "${KYLIN_HOME}/grafana/pid" ]]; then
        PID=`cat ${KYLIN_HOME}/grafana/pid`
        if ps -p $PID > /dev/null; then
          echo "Grafana is running, stop it first, PID is $PID"
          exit 1
        fi
    fi
    PORT=$(grep -w "http_port =" conf/defaults.ini |tr -d '[:space:]' | cut -d'=' -f2)

    used=`netstat -tpln | grep "\<$PORT\>" | awk '{print $7}' | sed "s/\// /g"`
    if [[ ! -z "$used" ]]; then
        echo "<$used> already listen on $PORT"
        exit 1
    fi

    echo "Grafana port is ${PORT}"

    metadata_url_prefix=`$KYLIN_HOME/bin/get-properties.sh kylin.metadata.url`
    ## check whether it contain '@' mark,if it exists, extract the content before it
    mark=`echo ${metadata_url_prefix} | grep "@"`
    if [ ${#mark} -ne 0 ]
    then
        metadata_url_prefix=`echo ${metadata_url_prefix} | awk -F'@' '{print $1}'`
    fi
    metrics_db_suffix=`$KYLIN_HOME/bin/get-properties.sh kylin.metrics.influx-db`
    metrics_daily_db_suffix=`$KYLIN_HOME/bin/get-properties.sh kylin.metrics.daily-influx-db`
    influxdb_https_enabled=`$KYLIN_HOME/bin/get-properties.sh kylin.influxdb.https.enabled`
    influxdb_unsafe_ssl_enabled=`$KYLIN_HOME/bin/get-properties.sh kylin.influxdb.https.unsafe-ssl.enabled`
    influxdb_unsafe_ssl_enabled=$([[ "${influxdb_https_enabled}" == "true" ]] && [[ "${influxdb_unsafe_ssl_enabled}" == "true" || -z "${influxdb_unsafe_ssl_enabled}" ]] && echo "true" || echo "false")

    export KE_METRICS_DATABASE=${metadata_url_prefix}_${metrics_db_suffix}
    export KE_METRICS_DAILY_DATABASE=${metadata_url_prefix}_${metrics_daily_db_suffix}
    export INFLUXDB_PROTOCOL=$([[ "$influxdb_https_enabled" == "true" ]] && echo "https" || echo "http")
    export INFLUXDB_ADDRESS=`$KYLIN_HOME/bin/get-properties.sh kylin.influxdb.address`
    export INFLUXDB_USERNAME=`$KYLIN_HOME/bin/get-properties.sh kylin.influxdb.username`
    export INFLUXDB_PASSWORD=`$KYLIN_HOME/bin/get-properties.sh kylin.influxdb.password DEC`

    ## The environment variable is a string, so use the sed command.
    sed -i "s/tlsSkipVerify: .*/tlsSkipVerify: ${influxdb_unsafe_ssl_enabled}/g" ${KYLIN_HOME}/grafana/conf/provisioning/datasources/kylin-influxdb.yaml
    sed -i "s/tlsSkipVerify: .*/tlsSkipVerify: ${influxdb_unsafe_ssl_enabled}/g" ${KYLIN_HOME}/grafana/conf/provisioning/datasources/kylin-influxdb-daily.yaml

    echo "Influxdb Connect Protocol: $INFLUXDB_PROTOCOL"
    echo "Influxdb Address: $INFLUXDB_ADDRESS"
    echo "Metrics Database: $KE_METRICS_DATABASE"

    if [[ -f "${KYLIN_HOME}/conf/grafana.ini" ]]; then
        nohup bin/grafana-server --config ${KYLIN_HOME}/conf/grafana.ini web > /dev/null 2>&1 &
    else
        nohup bin/grafana-server web > /dev/null 2>&1 &
    fi

    try_times=30
    while [[ ${try_times} -gt 0 ]];do
        sleep 3
        PID=`netstat -tpln 2>/dev/null | grep "\<$PORT\>" | awk '{print $7}' | sed "s/\// /g" | awk '{print $1}'`
        if [[ ! -z "${PID}" ]];then
            break
        fi
        let try_times-=1
    done

    if [[ ${try_times} -le 0 ]];then
        echo "Grafana start timeout."
        exit 1
    fi

    echo "$PID" > ${KYLIN_HOME}/grafana/pid

    echo "Grafana started, PID is $PID"
}

function stopGrafana(){
    if [[ -f "${KYLIN_HOME}/grafana/pid" ]]; then
        PID=`cat ${KYLIN_HOME}/grafana/pid`
        if ps -p "$PID" > /dev/null; then
           echo "Stopping Grafana: $PID"
           kill "$PID"
           for i in {1..10}; do
              sleep 3
              if ps -p "$PID" -f | grep grafana > /dev/null; then
                 if [[ "$i" == "10" ]]; then
                    echo "Killing Grafana: $PID"
                    kill -9 "$PID"
                 fi
                 continue
              fi
              break
           done
           rm ${KYLIN_HOME}/grafana/pid

           return 0
        else
           return 1
        fi
    else
        return 1
    fi

}

if [[ "$1" == "start" ]]; then
    echo "Starting Grafana..."
    startGrafana
elif [[ "$1" == "stop" ]]; then
    echo "Stopping Grafana..."
    stopGrafana
    if [[ $? == 0 ]]; then
        exit 0
    else
        echo "Grafana is not running."
        exit 1
    fi
else
    echo "Usage: 'grafana.sh start' or 'grafana.sh stop'"
    exit 1
fi