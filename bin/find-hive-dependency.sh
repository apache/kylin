#!/bin/sh

hive_env=`hive -e set | grep 'env:CLASSPATH'`

hive_classpath=`echo $hive_env | grep 'env:CLASSPATH' | awk -F '=' '{print $2}'`
arr=(`echo $hive_classpath | cut -d ":"  --output-delimiter=" " -f 1-`)
hive_exec_path=
for data in ${arr[@]}
do
    result=`echo $data | grep -e 'hive-exec[0-9\.-]*jar'`
    if [ $result ]
    then
        hive_exec_path=$data
    fi
done
hdp_home=`echo $hive_exec_path | awk -F '/hive/lib/' '{print $1}'`

hive_dependency=${hdp_home}/hive/conf:${hdp_home}/hive/lib/*

hcatalog=`find $hdp_home -name "hive-hcatalog-core[0-9\.-]*jar" 2>&1 | grep -m 1 -v 'Permission denied'`

if [ -z "$hcatalog" ]
then
    echo "hcatalog lib not found"
    exit 1
fi

hive_dependency=${hive_dependency}:${hcatalog}
echo "hive dependency: $hive_dependency"
export hive_dependency