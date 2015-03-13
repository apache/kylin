#!/bin/sh
dir=$(dirname ${0})
source ${dir}/check-env.sh
job_jar=`find ${KYLIN_HOME}/lib/ -name kylin-job*.jar`
cd ${KYLIN_HOME}/sample_cube/data
hive -f ${KYLIN_HOME}/sample_cube/create_sample_tables.sql  || { exit 1; }
cd ${KYLIN_HOME}
hbase org.apache.hadoop.util.RunJar ${job_jar} org.apache.kylin.common.persistence.ResourceTool upload ${KYLIN_HOME}/sample_cube/metadata  || { exit 1; }