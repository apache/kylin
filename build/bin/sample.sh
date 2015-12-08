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

dir=$(dirname ${0})
source ${dir}/check-env.sh
job_jar=`find -L ${KYLIN_HOME}/lib/ -name kylin-job*.jar`
echo "Going to create sample tables in hive..."
cd ${KYLIN_HOME}/sample_cube/data
hive -f ${KYLIN_HOME}/sample_cube/create_sample_tables.sql  || { exit 1; }
echo "Sample hive tables are created successfully; Going to create sample cube..."
cd ${KYLIN_HOME}
hbase org.apache.hadoop.util.RunJar ${job_jar} org.apache.kylin.common.persistence.ResourceTool upload ${KYLIN_HOME}/sample_cube/metadata  || { exit 1; }
echo "Sample cube is created successfully in project 'learn_kylin'; Restart Kylin server or reload the metadata from web UI to see the change."