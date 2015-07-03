#!/bin/sh

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
    if [ -z "$KYLIN_HOME" ]
    then
        echo 'please make sure KYLIN_HOME has been set'
        exit 1
    else
        echo "KYLIN_HOME is set to ${KYLIN_HOME}"
    fi

    dir=$(dirname ${0})

    #retrive $hive_dependency and $hbase_dependency
    source ${dir}/find-hive-dependency.sh
    source ${dir}/find-hbase-dependency.sh

    export HBASE_CLASSPATH=$hive_dependency:${HBASE_CLASSPATH}
    _monitorjar=`ls ${KYLIN_HOME}/lib |grep kylin-monitor`

    hbase \
    org.apache.hadoop.util.RunJar $KYLIN_HOME/lib/${_monitorjar} org.apache.kylin.monitor.Client
    exit 0