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

if [ -z "${JAVA_HOME}" ]
then
    JAVA_HOME=$(readlink -nf $(which java) | xargs dirname | xargs dirname | xargs dirname)
    if [ ! -e "$JAVA_HOME" ]  # nonexistent home
    then
        JAVA_HOME=""
    fi
    export JAVA_HOME=$JAVA_HOME
fi

# Validate kylin JDK version
JAVA_VERSION=$(java -version 2>&1  | awk -F '"' '/version/ {print $2}' | awk -F "." '{print $1$2}')
if [ "$JAVA_VERSION" -lt 18 ]
then
    quit "Kylin requires JDK 1.8+, please install or upgrade your JDK"
fi
