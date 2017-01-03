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

function checkCommandExits() {
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
    if [ -z "${kylin_versoin}" ]; then
        export kylin_version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -f kylin | grep -Ev '(^\[|Download\w+:)'`
        echo "Apache Kylin Version: ${kylin_version}"
    fi
    if [ -z "${release_version}" ]; then
        export release_version=$kap_version
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