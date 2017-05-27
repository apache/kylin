#!/bin/bash

# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

# source me
hive_conf_dir="${KYLIN_HOME}/conf/kylin_hive_conf.xml"

names=(`sed -n 's|<name>\(.*\)</name>|\1|p'  ${hive_conf_dir} | sed 's/ \+//g'`)
values=(`sed -n 's|<value>\(.*\)</value>|\1|p'  ${hive_conf_dir} | sed 's/ \+//g'`)

len_names=${#names[@]}
len_values=${#values[@]}

[[ $len_names == $len_values ]] || quit "Current hive conf file: ${hive_conf_dir} has inconsistent key value pairs."

for((i=0;i<$len_names;i++))
do
    hive_conf_properties=${hive_conf_properties}" --hiveconf ${names[$i]}=${values[$i]} "
done
