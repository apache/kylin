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

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo Retrieving hbase dependency...

hbase_classpath=`hbase classpath`

hadoop version | head -1 | grep cdh6
is_cdh6=$?

# special handling for Amazon EMR, to prevent re-init of hbase-setenv
is_aws=`uname -r | grep amzn`
if [ -n "$is_aws" ] && [ -d "/usr/lib/oozie/lib" ]; then
    export HBASE_ENV_INIT="true"
fi

arr=(`echo $hbase_classpath | cut -d ":" -f 1- | sed 's/:/ /g'`)
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
    if [[ $is_cdh6 -ne 0 ]]; then
    	quit "hbase-common lib not found"
    fi
fi

if [[ $is_cdh6 -eq 0 ]]; then
    hbase_dependency=${hbase_classpath}
else
    hbase_dependency=${hbase_common_path}
fi

verbose "hbase dependency: $hbase_dependency"
export hbase_dependency