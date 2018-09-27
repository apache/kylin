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


#check kylin home
if [ -z "$KYLIN_HOME" ]
then
    echo 'Please make sure KYLIN_HOME has been set'
    exit 1
else
    echo "KYLIN_HOME is set to ${KYLIN_HOME}"
fi


OUTPUT_FORDER=$KYLIN_HOME/system_cube

SC_NAME_1="KYLIN_HIVE_METRICS_QUERY_QA"
SC_NAME_2="KYLIN_HIVE_METRICS_QUERY_CUBE_QA"
SC_NAME_3="KYLIN_HIVE_METRICS_QUERY_RPC_QA"
SC_NAME_4="KYLIN_HIVE_METRICS_JOB_QA"
SC_NAME_5="KYLIN_HIVE_METRICS_JOB_EXCEPTION_QA"

if [ "$1" == "build" ]
then
    if [ -d "${OUTPUT_FORDER}" ]
    then
		sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_1} 600000 0
		sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_2} 600000 0
		sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_3} 600000 0
		sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_4} 600000 0
		sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_5} 600000 0
    else
    	echo "Please setup system cube first."
		exit 1
    fi
elif [ "$1" == "setup" ]
then
	#creat SCSinkTools.json
	SINK_TOOLS_FILE=$KYLIN_HOME/SCSinkTools.json
	cat <<- EOF > ${SINK_TOOLS_FILE}
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

    #add a crontab job
    CRONTAB_FILE=$KYLIN_HOME/crontabJob
    cat <<-EOF > ${CRONTAB_FILE}
    0 */4 * * * sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_1} 3600000 1200000
    20 */4 * * * sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_2} 3600000 1200000
    40 */8 * * * sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_3} 3600000 1200000
    30 */8 * * * sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_4} 3600000 1200000
    50 */24 * * * sh ${KYLIN_HOME}/bin/build-incremental-cube.sh ${SC_NAME_5} 3600000 1200000
	EOF
    crontab ${CRONTAB_FILE}
    rm ${CRONTAB_FILE}
else
    echo "usage: system-cube.sh setup"
    echo "       system-cube.sh build"
    exit 1
fi
