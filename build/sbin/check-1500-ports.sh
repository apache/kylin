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

#title=Checking Ports Availability

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

[[ -z $KYLIN_CONF ]] || quit "KYLIN_CONF should not be set. Please do: export KYLIN_CONF="

kylin_port=`$KYLIN_HOME/bin/get-properties.sh server.port`
if [[ -z ${kylin_port} ]]; then
    kylin_port=7070
fi

kylin_port_in_use=`netstat -tlpn | grep "\b${kylin_port}\b"`
[[ -z ${kylin_port_in_use} ]] || quit "ERROR: Port ${kylin_port} is in use, another Kyligence Enterprise server is running?"
