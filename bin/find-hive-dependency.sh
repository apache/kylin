#!/bin/sh

hive_env=`hive -e set | grep 'env:CLASSPATH'`

hive_classpath=`echo $hive_env | grep 'env:CLASSPATH' | awk -F '=' '{print $2}'`
arr=(`echo $hive_classpath | cut -d ":"  --output-delimiter=" " -f 1-`)
hive_exec_path=
for data in ${arr[@]}
do
    result=`echo $data | grep 'hive-exec.jar'`
    if [ $result ]
    then
        hive_exec_path=$data
    fi
done
hdp_home=`echo $hive_exec_path | awk -F '/hive/lib/' '{print $1}'`

hive_dependency=/usr/hdp/current/hive-client/conf/:${hdp_home}/hive/lib/*:${hdp_home}/hive-hcatalog/share/hcatalog/*
echo "hive dependency: $hive_dependency"
export hive_dependency