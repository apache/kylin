#!/bin/bash

# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

# source me

hive_conf_dir="${KYLIN_HOME}/conf/kylin_hive_conf.xml"
hive_conf_prop="${KYLIN_HOME}/logs/hive_props"
rm -rf ${hive_conf_prop}
export ENABLE_CHECK_ENV=false
${dir}/kylin.sh org.apache.kylin.tool.HiveConfigCLI ${hive_conf_dir} ${hive_conf_prop}
[[ 0 == $? ]] || quit "Can not parse xml file: ${hive_conf_dir}, please check it."
hive_conf_properties=`cat ${hive_conf_prop}`