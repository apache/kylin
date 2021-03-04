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

if [ -f "${dir}/cached-flink-dependency.sh" ] ; then
    source ${dir}/cached-flink-dependency.sh
    echo Using flink cached dependency...
fi


if [ -z "${flink_dependency}" ] ; then

    echo Retrieving Flink dependency...

    flink_home=

    if [ -n "$FLINK_HOME" ]
    then
        verbose "FLINK_HOME is set to: $FLINK_HOME, use it to locate Flink dependencies."
        flink_home=$FLINK_HOME
    fi

    if [ -z "$FLINK_HOME" ]
    then
        verbose "FLINK_HOME wasn't set, use $KYLIN_HOME/flink"
        flink_home=$KYLIN_HOME/flink
    fi

    if [ ! -d "$flink_home/lib" ]
    then
        echo `setColor 33 "Optional dependency flink not found, if you need this; set FLINK_HOME, or run bin/download-flink.sh"`
        echo "echo 'skip flink_dependency'" > ${dir}/cached-flink-dependency.sh
    else
        flink_dependency=`find -L $flink_home/lib -name '*.jar' ! -name '*shaded-hadoop*' ! -name 'kafka*' ! -name '*log4j*' ! -name '*slf4j*' ! -name '*calcite*' ! -name '*doc*' ! -name '*test*' ! -name '*sources*' ''-printf '%p:' | sed 's/:$//'`
        if [ -z "$flink_dependency" ]
        then
            quit "flink jars not found"
        else
            verbose "flink dependency: $flink_dependency"
            export flink_dependency
        fi
        echo "export flink_dependency=$flink_dependency" > ${dir}/cached-flink-dependency.sh
    fi
fi
