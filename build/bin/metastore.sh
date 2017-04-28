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
# It is designed to run in hadoop CLI, both in sandbox or in real hadoop environment
#
# If you're a developer of Kylin and want to download sandbox's metadata into your dev machine,
# take a look at SandboxMetastoreCLI


source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source ${dir}/check-env.sh "if-not-yet"

if [ "$1" == "backup" ]
then

    mkdir -p ${KYLIN_HOME}/meta_backups

    _now=$(date +"%Y_%m_%d_%H_%M_%S")
    _file="${KYLIN_HOME}/meta_backups/meta_${_now}"
    echo "Starting backup to ${_file}"
    mkdir -p ${_file}

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool download ${_file}
    echo "metadata store backed up to ${_file}"

elif [ "$1" == "fetch" ]
then

    _file=$2

    _now=$(date +"%Y_%m_%d_%H_%M_%S")
    _fileDst="${KYLIN_HOME}/meta_backups/meta_${_now}"
    echo "Starting restoring $_fileDst"
    mkdir -p $_fileDst

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool fetch $_fileDst $_file
    echo "metadata store backed up to $_fileDst"

elif [ "$1" == "restore" ]
then

    _file=$2
    echo "Starting restoring $_file"
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool upload $_file

elif [ "$1" == "list" ]
then

    _file=$2
    echo "Starting list $_file"
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool list $_file

elif [ "$1" == "remove" ]
then

    _file=$2
    echo "Starting remove $_file"
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool remove $_file

elif [ "$1" == "cat" ]
then

    _file=$2
    echo "Starting cat $_file"
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool cat $_file

elif [ "$1" == "reset" ]
then

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool  reset
    
elif [ "$1" == "refresh-cube-signature" ]
then

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.cube.cli.CubeSignatureRefresher
    
elif [ "$1" == "clean" ]
then

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.MetadataCleanupJob  "${@:2}"

else
    echo "usage: metastore.sh backup"
    echo "       metastore.sh fetch DATA"
    echo "       metastore.sh reset"
    echo "       metastore.sh refresh-cube-signature"
    echo "       metastore.sh restore PATH_TO_LOCAL_META"
    echo "       metastore.sh list RESOURCE_PATH"
    echo "       metastore.sh cat RESOURCE_PATH"
    echo "       metastore.sh remove RESOURCE_PATH"
    echo "       metastore.sh clean [--delete true]"
    exit 1
fi
