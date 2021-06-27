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


source ${KYLIN_HOME:-"$(cd -P -- "$(dirname -- "$0")" && pwd -P)/../"}/bin/header.sh

build_incremental_cube="${KYLIN_HOME}/bin/build-incremental-cube.sh"

function printHelp {
    echo "usage: system-cube.sh setup"
    echo "       system-cube.sh build [INTERVAL:600000] [DELAY:0]"
    echo "       system-cube.sh cron"
    echo "       system-cube.sh help"
    exit 1
}

function authorization {
    authed=$(grep %Auth% "$build_incremental_cube"|wc -l)
    if [ $authed -eq 1 ]
    then
        read -p $'Please Enter the ADMIN\'password or \'N\' to exit: ' pwd
        if [ $pwd == "N"  ]
        then
            return 1
        else
            base64_auth=$(echo -n "ADMIN:$pwd"|base64)
            tomcat_root=${dir}/../tomcat
            kylin_rest_address=`hostname -f`":"`grep "<Connector port=" ${tomcat_root}/conf/server.xml |grep protocol=\"HTTP/1.1\" | cut -d '=' -f 2 | cut -d \" -f 2`
            http_code=$(curl -I -m 10 -o /dev/null -s -w %{http_code} -X POST -H "Authorization: Basic $base64_auth" -H 'Content-Type: application/json' http://${kylin_rest_address}/kylin/api/user/authentication)
            if [ $http_code -eq 200  ]
            then
                sed -i "s/%Auth%/${base64_auth}/g" $build_incremental_cube
                return 0
            else
                echo `setColor 31 "Unauthorized,password error."`
                authorization
            fi
        fi
    else
        return 0
    fi
}

if [[ "$@" == *"help"* ]]
then
    printHelp
fi

OUTPUT_FORDER=$KYLIN_HOME/system_cube

KYLIN_ENV=`grep "^kylin.env=" $KYLIN_HOME/conf/kylin.properties | cut -d "=" -f 2`
KYLIN_ENV=${KYLIN_ENV:-"QA"}

KYLIN_METRICS_PREFIX=`grep "^kylin.metrics.prefix=" $KYLIN_HOME/conf/kylin.properties | cut -d "=" -f 2`
KYLIN_METRICS_PREFIX=${KYLIN_METRICS_PREFIX:-"KYLIN"}
KYLIN_METRICS_PREFIX=`echo "$KYLIN_METRICS_PREFIX"| tr '[a-z]' '[A-Z]'`

SC_NAME_1="KYLIN_HIVE_METRICS_QUERY_EXECUTION_${KYLIN_ENV}"
SC_NAME_2="KYLIN_HIVE_METRICS_QUERY_SPARK_JOB_${KYLIN_ENV}"
SC_NAME_3="KYLIN_HIVE_METRICS_QUERY_SPARK_STAGE_${KYLIN_ENV}"
SC_NAME_4="KYLIN_HIVE_METRICS_JOB_${KYLIN_ENV}"
SC_NAME_5="KYLIN_HIVE_METRICS_JOB_EXCEPTION_${KYLIN_ENV}"

if [ "$1" == "build" ]
then
    if [ -d "${OUTPUT_FORDER}" ]
    then
        BUILD_INTERVAL=${2:-"600000"}
        BUILD_DELAY=${3:-"0"}
        echo "build system cubes, build_interval:${BUILD_INTERVAL}, build_delay:${BUILD_DELAY}"
        sh $build_incremental_cube ${SC_NAME_1} ${BUILD_INTERVAL} ${BUILD_DELAY}
        sh $build_incremental_cube ${SC_NAME_2} ${BUILD_INTERVAL} ${BUILD_DELAY}
        sh $build_incremental_cube ${SC_NAME_3} ${BUILD_INTERVAL} ${BUILD_DELAY}
        sh $build_incremental_cube ${SC_NAME_4} ${BUILD_INTERVAL} ${BUILD_DELAY}
        sh $build_incremental_cube ${SC_NAME_5} ${BUILD_INTERVAL} ${BUILD_DELAY}
    else
    	  echo "Please setup system cube first."
		    exit 1
    fi
elif [ "$1" == "setup" ]
then
    #creat SCSinkTools.json
    SINK_TOOLS_FILE=$KYLIN_HOME/SCSinkTools.json

    echo "setup system cubes"

    rm -rf $SINK_TOOLS_FILE $OUTPUT_FORDER

cat <<-EOF > ${SINK_TOOLS_FILE}
[
  {
     "sink": "hive",
     "storage_type": 4,
     "cube_desc_override_properties": {
       "kylin.cube.max-building-segments": "1"
     }
  }
]
EOF

  $KYLIN_HOME/bin/kylin.sh org.apache.kylin.tool.metrics.systemcube.SCCreator \
  -inputConfig ${SINK_TOOLS_FILE} \
  -output ${OUTPUT_FORDER}

    hive_client_mode=`bash ${KYLIN_HOME}/bin/get-properties.sh kylin.source.hive.client`

  # Get Database
  system_database_tmp=`bash ${KYLIN_HOME}/bin/get-properties.sh kylin.metrics.prefix`
  system_database=${system_database_tmp:-"KYLIN"}
  system_database=`echo ${system_database} | tr [a-z] [A-Z]`

    # 'create database' failed will not exit when donot have permission to create database;
    sed -i -e 's/CREATE DATABASE /-- CREATE DATABASE /g' ${OUTPUT_FORDER}/create_hive_tables_for_system_cubes.sql

    if [ "${hive_client_mode}" == "beeline" ]
    then
        beeline_params=`bash ${KYLIN_HOME}/bin/get-properties.sh kylin.source.hive.beeline-params`
        beeline ${beeline_params} -e "CREATE DATABASE IF NOT EXISTS "$system_database

        hive2_url=`expr match "${beeline_params}" '.*\(hive2:.*:[0-9]\{4,6\}\/\)'`
        if [ -z ${hive2_url} ]; then
            hive2_url=`expr match "${beeline_params}" '.*\(hive2:.*:[0-9]\{4,6\}\)'`
            beeline_params=${beeline_params/${hive2_url}/${hive2_url}/${system_database}}
        else
            beeline_params=${beeline_params/${hive2_url}/${hive2_url}${system_database}}
        fi

        beeline ${beeline_params} -f ${OUTPUT_FORDER}/create_hive_tables_for_system_cubes.sql  || { exit 1; }
    else
        hive -e "CREATE DATABASE IF NOT EXISTS "$system_database
        hive --database $system_database -f ${OUTPUT_FORDER}/create_hive_tables_for_system_cubes.sql  || { exit 1; }
    fi

    $KYLIN_HOME/bin/metastore.sh restore ${OUTPUT_FORDER}

    #refresh signature
    $KYLIN_HOME/bin/kylin.sh org.apache.kylin.cube.cli.CubeSignatureRefresher ${SC_NAME_1},${SC_NAME_2},${SC_NAME_3},${SC_NAME_4},${SC_NAME_5}

elif [ "$1" == "cron" ]
then
    #check exists
    cron_count=$(crontab -l | grep "${KYLIN_METRICS_PREFIX}_HIVE_METRICS" | wc -l)
    if [ $cron_count -eq 5 ]
    then
        echo `setColor 33 "system cube already exists in crontab"`
        exit 0
    else
        #add a crontab job
        echo "add to a crontab job"
        authorization
        if [[ $? == 1 ]]
        then
            echo "add to a crontab job exit."
            exit 0
        else
            CRONTAB_FILE=$KYLIN_HOME/crontabJob
            crontab -l >> ${CRONTAB_FILE}

cat <<-EOF >> ${CRONTAB_FILE}
0 */2 * * * sh $build_incremental_cube ${SC_NAME_1} 3600000 1200000
20 */2 * * * sh $build_incremental_cube ${SC_NAME_2} 3600000 1200000
40 */4 * * * sh $build_incremental_cube ${SC_NAME_3} 3600000 1200000
30 */4 * * * sh $build_incremental_cube ${SC_NAME_4} 3600000 1200000
50 */12 * * * sh $build_incremental_cube ${SC_NAME_5} 3600000 1200000
EOF
            crontab ${CRONTAB_FILE}
            rm ${CRONTAB_FILE}
            echo "add to a crontab job successful."
        fi
    fi
else
    printHelp
fi
