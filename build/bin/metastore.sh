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

# This script is for production metadata store manipulation
# It is desinged to run in hadoop CLI, both in sandbox or in real hadoop environment
#
# If you're a developper of Kylin and want to download sandbox's metadata into your dev machine,
# take a look at SandboxMetastoreCLI


dir=$(dirname ${0})
source ${dir}/check-env.sh

if [ "$1" == "backup" ]
then

    mkdir -p ${KYLIN_HOME}/meta_backups

    _now=$(date +"%Y_%m_%d_%H_%M_%S")
    _file="${KYLIN_HOME}/meta_backups/meta_${_now}"
    echo "Starting backup to ${_file}"
    mkdir -p ${_file}

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool download ${_file}
    echo "metadata store backed up to ${_file}"

elif [ "$1" == "restore" ]
then

    _file=$2
    echo "Starting restoring $_file"
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool upload $_file

elif [ "$1" == "reset" ]
then

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool  reset
    
elif [ "$1" == "clean" ]
then

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.engine.mr.steps.MetadataCleanupJob "${@:2}"

else
    echo "usage: metastore.sh backup"
    echo "       metastore.sh reset"
    echo "       metastore.sh restore PATH_TO_LOCAL_META"
    echo "       metastore.sh clean [--delete true]"
    exit 1
fi
