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

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh "$@"
mkdir -p ${KYLIN_HOME}/logs
ERR_LOG=${KYLIN_HOME}/logs/shell.stderr
OUT_LOG=${KYLIN_HOME}/logs/shell.stdout
echo "-----------------------  log start  -----------------------" >>${ERR_LOG}
echo "-----------------------  log start  -----------------------" >>${OUT_LOG}
bash -x ${KYLIN_HOME}/sbin/bootstrap.sh "$@" 2>>${ERR_LOG}  | tee -a ${OUT_LOG}
ret=${PIPESTATUS[0]}
echo "-----------------------  log end  -------------------------" >>${ERR_LOG}
echo "-----------------------  log end  -------------------------" >>${OUT_LOG}
exit ${ret}