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

#title=Checking OS Commands

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking OS commands..."

command -v lsb_release                  || echo "${CHECKENV_REPORT_PFX}WARN: Command lsb_release is not accessible. Please run on Linux OS."
a=`lsb_release -a`                      || echo "${CHECKENV_REPORT_PFX}WARN: Command 'lsb_release -a' does not work. Please run on Linux OS."
[[ $a == *Mac* ]]                       && echo "${CHECKENV_REPORT_PFX}WARN: Mac is not officially supported. Use at your own risk."
[[ $a == *Ubuntu* ]]                    && echo "${CHECKENV_REPORT_PFX}WARN: Ubuntu is not officially supported. Use at your own risk."

command -v hadoop                       || quit "ERROR: Command 'hadoop' is not accessible. Please check Hadoop client setup."
if [[ $(hadoop version) != *"mapr"* ]]
then
    command -v hdfs                         || quit "ERROR: Command 'hdfs' is not accessible. Please check Hadoop client setup."
fi
command -v yarn                         || quit "ERROR: Command 'yarn' is not accessible. Please check Hadoop client setup."