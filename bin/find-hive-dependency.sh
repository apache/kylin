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
hdp_home=`echo $hive_exec_path | awk -F '/hive.*/lib/' '{print $1}'`

hcatalog=`find $hdp_home -name "hive-hcatalog-core[0-9\.-]*jar" 2>&1 | grep -m 1 -v 'Permission denied'`
hive_lib=`find "$(dirname $hive_exec_path)" -name '*.jar' ! -name '*calcite*' -printf '%p:' | sed 's/:$//'`

if [ -z "$hcatalog" ]
then
    echo "hcatalog lib not found"
    exit 1
fi

hive_dependency=${hive_conf_path}:${hive_lib}:${hcatalog}
echo "hive dependency: $hive_dependency"
export hive_dependency
