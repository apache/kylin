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

dir=$(dirname ${0})
source ${dir}/check-env.sh

_jobjar=`ls ${KYLIN_HOME}/lib |grep kylin-job`
_fulljobjar="${KYLIN_HOME}/lib/${_jobjar}"

if [ $1 == "backup" ]
then

    mkdir -p ${KYLIN_HOME}/meta_backups

    _now=$(date +"%Y_%m_%d_%H_%M_%S")
    _file="${KYLIN_HOME}/meta_backups/meta_${_now}"
    echo "Starting backup to ${_file}"
    mkdir -p ${_file}

    hbase  org.apache.hadoop.util.RunJar ${_fulljobjar}   org.apache.kylin.common.persistence.ResourceTool  copy ${KYLIN_HOME}/conf/kylin.properties ${_file}
    echo "metadata store backed up to ${_file}"

elif [ $1 == "restore" ]
then

    _file=$2
    echo "Starting restoring $_file"
    hbase  org.apache.hadoop.util.RunJar  ${_fulljobjar}   org.apache.kylin.common.persistence.ResourceTool  copy $_file ${KYLIN_HOME}/conf/kylin.properties

elif [ $1 == "reset" ]
then

    hbase  org.apache.hadoop.util.RunJar ${_fulljobjar}   org.apache.kylin.common.persistence.ResourceTool  reset

else
    echo "usage: metastore.sh backup or metastore.sh restore PATH_TO_LOCAL_META"
    exit 1
fi
