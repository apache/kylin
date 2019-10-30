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


source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

function printHelp {
    echo "usage: system-cube.sh setup"
    echo "       system-cube.sh build [INTERVAL:600000] [DELAY:0]"
    echo "       system-cube.sh cron"
    echo "       system-cube.sh help"
    exit 1
}

if [[ "$@" == *"help"* ]]
then
    printHelp
fi

OUTPUT_FORDER=$KYLIN_HOME/system_cube

KYLIN_ENV=`grep "^kylin.env=" $KYLIN_HOME/conf/kylin.properties | cut -d "=" -f 2`
KYLIN_ENV=${KYLIN_ENV:-"QA"}

SC_NAME_1="KYLIN_HIVE_METRICS_QUERY_${KYLIN_ENV}"
SC_NAME_2="KYLIN_HIVE_METRICS_QUERY_CUBE_${KYLIN_ENV}"
SC_NAME_3="KYLIN_HIVE_METRICS_QUERY_RPC_${KYLIN_ENV}"
SC_NAME_4="KYLIN_HIVE_METRICS_JOB_${KYLIN_ENV}"
SC_NAME_5="KYLIN_HIVE_METRICS_JOB_EXCEPTION_${KYLIN_ENV}"

if [ "$1" == "build" ]
then
    if [ -d "${OUTPUT_FORDER}" ]
    then
        BUILD_INTERVAL=${2:-"600000"}
        BUILD_DELAY=${3:-"0"}

        echo "build system cubes, build_interval:${BUILD_INTERVAL}, build_delay:${BUILD_DELAY}"

		sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_1} ${BUILD_INTERVAL} ${BUILD_DELAY}
		sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_2} ${BUILD_INTERVAL} ${BUILD_DELAY}
		sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_3} ${BUILD_INTERVAL} ${BUILD_DELAY}
		sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_4} ${BUILD_INTERVAL} ${BUILD_DELAY}
		sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_5} ${BUILD_INTERVAL} ${BUILD_DELAY}
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
	  [
		"org.apache.kylin.tool.metrics.systemcube.util.HiveSinkTool",
		{
		  "storage_type": 2,
		  "cube_desc_override_properties": [
			"java.util.HashMap",
			{
			  "kylin.cube.algorithm": "INMEM",
			  "kylin.cube.max-building-segments": "1"
			}
		  ]
		}
	  ]
	]
	EOF
    $KYLIN_HOME/bin/kylin.sh org.apache.kylin.tool.metrics.systemcube.SCCreator \
    -inputConfig ${SINK_TOOLS_FILE} \
    -output ${OUTPUT_FORDER}

    hive -f ${OUTPUT_FORDER}/create_hive_tables_for_system_cubes.sql

    $KYLIN_HOME/bin/metastore.sh restore ${OUTPUT_FORDER}

    #refresh signature
    $KYLIN_HOME/bin/kylin.sh org.apache.kylin.cube.cli.CubeSignatureRefresher ${SC_NAME_1},${SC_NAME_2},${SC_NAME_3},${SC_NAME_4},${SC_NAME_5}
elif [ "$1" == "cron" ]
then
    #add a crontab job
    echo "add to a crontab job"

    CRONTAB_FILE=$KYLIN_HOME/crontabJob
    	crontab -l >> ${CRONTAB_FILE}
	cat <<-EOF >> ${CRONTAB_FILE}
    0 */2 * * * sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_1} 3600000 1200000
    20 */2 * * * sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_2} 3600000 1200000
    40 */4 * * * sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_3} 3600000 1200000
    30 */4 * * * sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_4} 3600000 1200000
    50 */12 * * * sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_5} 3600000 1200000
	EOF
    crontab ${CRONTAB_FILE}
    rm ${CRONTAB_FILE}
else
    printHelp
fi
