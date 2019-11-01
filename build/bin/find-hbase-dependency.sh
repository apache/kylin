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

echo Retrieving hbase dependency...

hbase_classpath=`hbase classpath`

# special handling for Amazon EMR, to prevent re-init of hbase-setenv
is_aws=`uname -r | grep amzn`
if [ -n "$is_aws" ] && [ -d "/usr/lib/oozie/lib" ]; then
    export HBASE_ENV_INIT="true"
fi

hbase_dependency=${hbase_classpath}
verbose "hbase dependency: $hbase_dependency"
export hbase_dependency
echo "export HBASE_ENV_INIT=$HBASE_ENV_INIT
export hbase_dependency=$hbase_dependency" > ${dir}/cached-hbase-dependency.sh
