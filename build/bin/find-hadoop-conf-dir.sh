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

echo Retrieving hadoop conf dir...

override_hadoop_conf_dir=`bash ${KYLIN_HOME}/bin/get-properties.sh kylin.env.hadoop-conf-dir`

if [ -n "$override_hadoop_conf_dir" ]; then
    echo "$override_hadoop_conf_dir is override as the kylin_hadoop_conf_dir"
    export kylin_hadoop_conf_dir=${override_hadoop_conf_dir}
    return
fi

hbase_classpath=`hbase classpath`

arr=(`echo $hbase_classpath | cut -d ":" -f 1- | sed 's/:/ /g'`)
kylin_hadoop_conf_dir=

for data in ${arr[@]}
do
    result=`echo $data | grep -v -E ".*jar"`
    if [ $result ]
    then
        valid_conf_dir=true
        
        if [ ! -f $result/yarn-site.xml ]
        then
            verbose "$result is not valid hadoop dir conf because yarn-site.xml is missing"
            valid_conf_dir=false
            continue
        fi
        
        if [ ! -f $result/mapred-site.xml ]
        then
            verbose "$result is not valid hadoop dir conf because mapred-site.xml is missing"
            valid_conf_dir=false
            continue
        fi
        
        if [ ! -f $result/hdfs-site.xml ]
        then
            verbose "$result is not valid hadoop dir conf because hdfs-site.xml is missing"
            valid_conf_dir=false
            continue
        fi
        
        if [ ! -f $result/core-site.xml ]
        then
            verbose "$result is not valid hadoop dir conf because core-site.xml is missing"
            valid_conf_dir=false
            continue
        fi
        
        verbose "$result is chosen as the kylin_hadoop_conf_dir"
        export kylin_hadoop_conf_dir=$result
        return
    fi
done

