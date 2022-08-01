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

function fetchFIZkInfo(){
    if [ -n "$FI_ENV_PLATFORM" ]
    then
        CLIENT_HIVE_URI=`env | grep "CLIENT_HIVE_URI"`
        array=(${CLIENT_HIVE_URI//// })
        verbose "FI_ZK_CONNECT_INFO is:${array[1]}"
        FI_ZK_CONNECT_INFO=${array[1]}

        zkConnectString=`$KYLIN_HOME/bin/get-properties.sh kylin.env.zookeeper-connect-string`
        if [ -z "$zkConnectString" ]
        then
            sed -i '$a\kylin.env.zookeeper-connect-string='$FI_ZK_CONNECT_INFO'' ${KYLIN_CONFIG_FILE}
        fi
    fi
}