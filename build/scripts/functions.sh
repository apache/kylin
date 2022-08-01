#!/usr/bin/bash

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

function checkCommandExists() {
    echo "Checking ${1}..."
    if [ -z "$(command -v ${1})" ]
    then
        echo "Please install ${1} first so that Kylin packaging can proceed"
        exit 1
    else
        echo "${1} check passed"
    fi
}

function exportProjectVersions() {
    if [ -z "${kap_version}" ]; then
        export kylin_version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -E '^[0-9]+\.[0-9]+\.[0-9]+' `
        echo "KAP Version: ${kylin_version}"
    fi
    if [ -z "${release_version}" ]; then
        export release_version=$kylin_version
    fi
}

function detectOSType() {
    OS_TYPE="linux"
    if [[ `uname -a` =~ "Darwin" ]]; then
        OS_TYPE="mac"
    elif [[ `uname -a` =~ "Cygwin" ]]; then
        OS_TYPE="windows"
    fi
    echo $OS_TYPE
}

function calMd5() {
    OS_TYPE=`detectOSType`
    if [[ "$OS_TYPE" == "mac" ]]; then
        md5 -q $1
    elif [[ "$OS_TYPE" == "windows" ]]; then
        md5sum $1
    else
        md5sum $1
    fi
}

function checkDownloadSparkVersion() {
    if [[ -z $1 ]];
    then
        echo "spark version check failed, download spark version parameter is null"
        exit 1
    fi

    download_spark_release_version=$1
    spark_version_pom=`mvn help:evaluate -Dexpression=spark.version | grep -E '^[0-9]+\.[0-9]+\.[0-9]+' `
    pom_spark_release_version=spark-newten-"`echo ${spark_version_pom}| sed "s/-kylin//g"`"

    if [[ $download_spark_release_version != $pom_spark_release_version ]];
    then
        echo "spark version check failed, download version is ${download_spark_release_version}, but ${pom_spark_release_version} in pom file"
        exit 1
    fi
}
