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
source ${KYLIN_HOME:-"$(cd -P -- "$(dirname -- "$0")" && pwd -P)/../"}/bin/util.sh

# get port from configuraton
kylin_port=`grep "<Connector port=" ${KYLIN_HOME}/tomcat/conf/server.xml |grep protocol=\"HTTP/1.1\" | cut -d '=' -f 2 | cut -d \" -f 2`

# check the availability of the port
if isMacosX; then
    kylin_port_in_use=`lsof -nP -iTCP:"${kylin_port}" | grep LISTEN`
else
    kylin_port_in_use=`netstat -tln | grep "\b${kylin_port}\b"`
fi

# if not available, prompt error messeage
[[ -z ${kylin_port_in_use} ]] || quit "ERROR: Port ${kylin_port} is in use, please check the availability of the port and re-start Kylin"
