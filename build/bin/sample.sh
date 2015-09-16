#!/bin/bash
dir=$(dirname ${0})
source ${dir}/check-env.sh
job_jar=`find ${KYLIN_HOME}/lib/ -name kylin-job*.jar`
echo "Going to create sample tables in hive..."
cd ${KYLIN_HOME}/sample_cube/data
hive -f ${KYLIN_HOME}/sample_cube/create_sample_tables.sql  || { exit 1; }
echo "Sample hive tables are created successfully; Going to create sample cube..."
cd ${KYLIN_HOME}
hbase org.apache.hadoop.util.RunJar ${job_jar} org.apache.kylin.common.persistence.ResourceTool upload ${KYLIN_HOME}/sample_cube/metadata  || { exit 1; }
echo "Sample cube is created successfully in project 'learn_kylin'; Restart Kylin server or reload the metadata from web UI to see the change."