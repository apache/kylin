#!/bin/bash

hbase_classpath=`hbase classpath`
arr=(`echo $hbase_classpath | cut -d ":"  --output-delimiter=" " -f 1-`)
hbase_common_path=
for data in ${arr[@]}
do
    result=`echo $data | grep -e 'hbase-common[a-z0-9A-Z\.-]*jar' | grep -v tests`
    if [ $result ]
    then
        hbase_common_path=$data
    fi
done

if [ -z "$hbase_common_path" ]
then
    echo "hbase-common lib not found"
    exit 1
fi

hbase_dependency=${hbase_common_path}
echo "hbase dependency: $hbase_dependency"
export hbase_dependency