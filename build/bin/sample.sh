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

dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
export KYLIN_HOME=${KYLIN_HOME:-"${dir}/../"}

source ${dir}/check-env.sh
job_jar=`find -L ${KYLIN_HOME}/lib/ -name kylin-job*.jar`

echo "Going to create sample tables in hive..."
cd ${KYLIN_HOME}/sample_cube/data

echo "Loading sample data into HDFS tmp path: /tmp/kylin/sample_cube/data"
hadoop fs -mkdir -p /tmp/kylin/sample_cube/data
hadoop fs -put * /tmp/kylin/sample_cube/data/

hive_client_mode=`sh ${KYLIN_HOME}/bin/get-properties.sh kylin.hive.client`
if [ "${hive_client_mode}" == "beeline" ]
then
    beeline_params=`sh ${KYLIN_HOME}/bin/get-properties.sh kylin.hive.beeline.params`
    beeline ${beeline_params} -f ${KYLIN_HOME}/sample_cube/create_sample_tables.sql  || { exit 1; }
else
    hive -f ${KYLIN_HOME}/sample_cube/create_sample_tables.sql  || { exit 1; }
fi

echo "Sample hive tables are created successfully; Going to create sample cube..."
hadoop fs -rm -r /tmp/kylin/sample_cube

# set engine type and storage type to cube desc
default_engine_type=`sh ${KYLIN_HOME}/bin/get-properties.sh kylin.default.cube.engine`
default_storage_type=`sh ${KYLIN_HOME}/bin/get-properties.sh kylin.default.storage.engine`
if [ -z "$default_engine_type" ]; then
    default_engine_type=2
    default_storage_type=2
fi

mkdir -p ${KYLIN_HOME}/sample_cube/metadata
cp -rf ${KYLIN_HOME}/sample_cube/template/* ${KYLIN_HOME}/sample_cube/metadata

sed -i "s/%default_storage_type%/${default_storage_type}/g" ${KYLIN_HOME}/sample_cube/metadata/cube_desc/kylin_sales_cube_desc.json
sed -i "s/%default_engine_type%/${default_engine_type}/g" ${KYLIN_HOME}/sample_cube/metadata/cube_desc/kylin_sales_cube_desc.json

cd ${KYLIN_HOME}
hbase org.apache.hadoop.util.RunJar ${job_jar} org.apache.kylin.common.persistence.ResourceTool upload ${KYLIN_HOME}/sample_cube/metadata  || { exit 1; }
echo "Sample cube is created successfully in project 'learn_kylin'."
echo "Restart Kylin server or reload the metadata from web UI to see the change."
