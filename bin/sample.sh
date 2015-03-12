#!/bin/sh
dir=$(dirname ${0})
job_jar=`ls -ls ${KYLIN_HOME}/lib | grep kylin-job | awk '{print $9}'`
cd ${KYLIN_HOME}/sample_cube/data
hive -f ${KYLIN_HOME}/sample_cube/create_sample_tables.sql  || { exit 1; }
hbase org.apache.hadoop.util.RunJar ${KYLIN_HOME}/lib/${job_jar} org.apache.kylin.common.persistence.ResourceTool upload ${KYLIN_HOME}/sample_cube/metadata  || { exit 1; }