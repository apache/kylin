#!/bin/bash

hive_env=`hive -e set | grep 'env:CLASSPATH'`

hive_classpath=`echo $hive_env | grep 'env:CLASSPATH' | awk -F '=' '{print $2}'`
arr=(`echo $hive_classpath | cut -d ":"  --output-delimiter=" " -f 1-`)
hive_conf_path=
hive_exec_path=
for data in ${arr[@]}
do
    result=`echo $data | grep -e 'hive-exec[a-z0-9A-Z\.-]*jar'`
    if [ $result ]
    then
        hive_exec_path=$data
    fi
    result=`echo $data | grep -e 'hive[^/]*/conf'`
    if [ $result ]
    then
        hive_conf_path=$data
    fi
done
hive_home=`echo $hive_exec_path | awk -F '/lib/' '{print $1}'`/

hcatalog=`find $hive_home -name "hive-hcatalog-core[0-9\.-]*jar" 2>&1 | grep -m 1 -v 'Permission denied'`
hive_lib=`find "$(dirname $hive_exec_path)" -name '*.jar' ! -name '*calcite*' -printf '%p:' | sed 's/:$//'`

if [ -z "$hcatalog" ]
then
    echo "hcatalog lib not found"
    exit 1
fi

hive_dependency=${hive_conf_path}:${hive_lib}:${hcatalog}
echo "hive dependency: $hive_dependency"
export hive_dependency
