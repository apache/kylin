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
source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/find-hadoop-conf-dir.sh

source ${dir}/check-env.sh "if-not-yet"
job_jar=`find -L ${KYLIN_HOME}/lib/ -name kylin-job*.jar`

cd ${KYLIN_HOME}/sample_cube/data

if [ -z "${kylin_hadoop_conf_dir}" ]; then
    hadoop_conf_param=
else
    hadoop_conf_param="--config ${kylin_hadoop_conf_dir}"
fi

echo "Loading sample data into HDFS tmp path: /tmp/kylin/sample_cube/data"
hadoop ${hadoop_conf_param} fs -mkdir -p /tmp/kylin/sample_cube/data
hadoop ${hadoop_conf_param} fs -put * /tmp/kylin/sample_cube/data/

hive_client_mode=`bash ${KYLIN_HOME}/bin/get-properties.sh kylin.source.hive.client`
sample_database=`bash ${KYLIN_HOME}/bin/get-properties.sh kylin.source.hive.database-for-flat-table`
sample_database=${sample_database^^}
echo "Going to create sample tables in hive to database "$sample_database" by "$hive_client_mode

if [ "${hive_client_mode}" == "beeline" ]
then
    beeline_params=`bash ${KYLIN_HOME}/bin/get-properties.sh kylin.source.hive.beeline-params`
    beeline ${beeline_params} -e "CREATE DATABASE IF NOT EXISTS "$sample_database
    hive2_url=`expr match "${beeline_params}" '.*\(hive2:.*:[0-9]\{4,6\}\/\)'`
    if [ -z ${hive2_url} ]; then
        hive2_url=`expr match "${beeline_params}" '.*\(hive2:.*:[0-9]\{4,6\}\)'`
        beeline_params=${beeline_params/${hive2_url}/${hive2_url}/${sample_database}}
    else
        beeline_params=${beeline_params/${hive2_url}/${hive2_url}${sample_database}}
    fi
    beeline ${beeline_params} -f ${KYLIN_HOME}/sample_cube/create_sample_tables.sql  || { exit 1; }
else
    hive -e "CREATE DATABASE IF NOT EXISTS "$sample_database
    hive --database $sample_database -f ${KYLIN_HOME}/sample_cube/create_sample_tables.sql  || { exit 1; }
fi

echo "Sample hive tables are created successfully; Going to create sample cube..."
hadoop ${hadoop_conf_param} fs -rm -r /tmp/kylin/sample_cube

# set engine type and storage type to cube desc
default_engine_type=`bash ${KYLIN_HOME}/bin/get-properties.sh kylin.engine.default`
default_storage_type=`bash ${KYLIN_HOME}/bin/get-properties.sh kylin.storage.default`
if [ -z "$default_engine_type" ]; then
    default_engine_type=2
    default_storage_type=2
fi

mkdir -p ${KYLIN_HOME}/sample_cube/metadata
cp -rf ${KYLIN_HOME}/sample_cube/template/* ${KYLIN_HOME}/sample_cube/metadata

sed -i "s/%default_storage_type%/${default_storage_type}/g" ${KYLIN_HOME}/sample_cube/metadata/cube_desc/kylin_sales_cube.json
sed -i "s/%default_engine_type%/${default_engine_type}/g" ${KYLIN_HOME}/sample_cube/metadata/cube_desc/kylin_sales_cube.json

#### Replace the 'DEFAULT' with kylin.source.hive.database-for-flat-table
sed -i "s/DEFAULT./$sample_database./g" ${KYLIN_HOME}/sample_cube/metadata/cube_desc/kylin_sales_cube.json
sed -i "s/DEFAULT./$sample_database./g" ${KYLIN_HOME}/sample_cube/metadata/model_desc/kylin_sales_model.json
sed -i "s/DEFAULT./$sample_database./g" ${KYLIN_HOME}/sample_cube/metadata/project/learn_kylin.json
sed -i "s/DEFAULT/$sample_database/g" ${KYLIN_HOME}/sample_cube/metadata/table/*.json
cd ${KYLIN_HOME}/sample_cube/metadata/table
ls -1 DEFAULT.KYLIN_*.json|sed "s/\(DEFAULT\)\(.*\)\.json/mv & $sample_database\2.json/"|sh -v

cd ${KYLIN_HOME}
hbase org.apache.hadoop.util.RunJar ${job_jar} org.apache.kylin.common.persistence.ResourceTool upload ${KYLIN_HOME}/sample_cube/metadata  || { exit 1; }
echo "Sample cube is created successfully in project 'learn_kylin'."
echo "Restart Kylin server or reload the metadata from web UI to see the change."
