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

source ${KYLIN_HOME:-"$(cd -P -- "$(dirname -- "$0")" && pwd -P)/../"}/bin/header.sh

echo Retrieving hadoop conf dir...

function find_hadoop_conf_dir() {
    if [[ -d ${kylin_hadoop_conf_dir} ]]; then
        return
    fi

    override_hadoop_conf_dir=`bash ${KYLIN_HOME}/bin/get-properties.sh kylin.env.hadoop-conf-dir`
    
    if [ -n "$override_hadoop_conf_dir" ]; then
        verbose "hadoop_conf_dir is override as $override_hadoop_conf_dir"
        export hadoop_conf_dir=${override_hadoop_conf_dir}
    else
        hadoop_classpath=`hadoop classpath`

        arr=(`echo $hadoop_classpath | cut -d ":" -f 1- | sed 's/:/ /g'`)
        hadoop_conf_dir=

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

                verbose "hadoop_conf_dir is $result"
                export hadoop_conf_dir=$result

                break
            fi
        done
        fi

        if [ -d "$hadoop_conf_dir" ]
        then
            mkdir -p ${KYLIN_HOME}/hadoop_conf

            checkAndLinkFile $hadoop_conf_dir/core-site.xml $kylin_hadoop_conf_dir/core-site.xml
            checkAndLinkFile $hadoop_conf_dir/hdfs-site.xml $kylin_hadoop_conf_dir/hdfs-site.xml
            checkAndLinkFile $hadoop_conf_dir/yarn-site.xml $kylin_hadoop_conf_dir/yarn-site.xml
            checkAndLinkFile $hadoop_conf_dir/mapred-site.xml $kylin_hadoop_conf_dir/mapred-site.xml

            # For CDH
            checkAndLinkFile $hadoop_conf_dir/topology.py $kylin_hadoop_conf_dir/topology.py
            checkAndLinkFile $hadoop_conf_dir/topology.map $kylin_hadoop_conf_dir/topology.map
            # For HDP
            checkAndLinkFile $hadoop_conf_dir/topology_script.py $kylin_hadoop_conf_dir/topology_script.py
            checkAndLinkFile $hadoop_conf_dir/topology_mappings.data $kylin_hadoop_conf_dir/topology_mappings.data

            checkAndLinkFile $hadoop_conf_dir/ssl-client.xml $kylin_hadoop_conf_dir/ssl-client.xml
            checkAndLinkFile $hadoop_conf_dir/hadoop-env.sh $kylin_hadoop_conf_dir/hadoop-env.sh

            copyHiveSite
        fi
}

function checkAndLinkFile() {
    source_file=$1
    if [[ -f ${source_file} ]]; then
        dst_file=$2
        ln -s ${source_file} ${dst_file}
    fi
}

function copyHiveSite() {
    if [ -n "$HIVE_CONF" ]
    then
        if [ -f "$HIVE_CONF/hive-site.xml" ]
        then
            hive_site=$HIVE_CONF/hive-site.xml
        fi
    elif [ -n "$HIVE_HOME" ]
    then
        if [ -f "$HIVE_HOME/conf/hive-site.xml" ]
        then
            hive_site=$HIVE_HOME/conf/hive-site.xml
        fi
    elif [ -f /etc/hive/conf/hive-site.xml ]
    then
        hive_site=/etc/hive/conf/hive-site.xml
    fi

    if [ -n "$hive_site" ]
    then
        ln -s "${hive_site}" "${kylin_hadoop_conf_dir}/hive-site.xml"
    else
        echo "hive-site.xml is missing"
    fi
    return
}

export kylin_hadoop_conf_dir=${KYLIN_HOME}/hadoop_conf
find_hadoop_conf_dir

echo "export kylin_hadoop_conf_dir=$kylin_hadoop_conf_dir" > ${dir}/cached-hadoop-conf-dir.sh