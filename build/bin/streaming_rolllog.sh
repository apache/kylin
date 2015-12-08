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

source /etc/profile
source ~/.bash_profile

KYLIN_LOG_HOME=${KYLIN_HOME}/logs
cd ${KYLIN_LOG_HOME}
timestamp=`date +%Y_%m_%d_%H_%M_%S`
tarfile=logs_archived_at_${timestamp}.tar
files=`find -L . ! -name '*.tar' -type f -mtime +1` # keep two days' log
echo ${files} | xargs tar -cvf ${tarfile}
echo ${files} | xargs rm