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

if [ -f "${dir}/cached-spark-dependency.sh" ] ; then
    source ${dir}/cached-spark-dependency.sh
    echo Using spark cached dependency...
fi

if [ -z "${spark_dependency}" ] ; then

    echo Retrieving Spark dependency...

    spark_home=

    if [ -n "$SPARK_HOME" ]
    then
        verbose "SPARK_HOME is set to: $SPARK_HOME, use it to locate Spark dependencies."
        spark_home=$SPARK_HOME
    fi

    if [ -z "$SPARK_HOME" ]
    then
        verbose "SPARK_HOME wasn't set, use $KYLIN_HOME/spark"
        spark_home=$KYLIN_HOME/spark
    fi

    SPARK_EVENTLOG_DIR=`bash $KYLIN_HOME/bin/get-properties.sh kylin.engine.spark-conf.spark.eventLog.dir`
    if [ -n "$SPARK_EVENTLOG_DIR" ]
    then
        hadoop ${hadoop_conf_param} fs -mkdir -p $SPARK_EVENTLOG_DIR
        if [ $? != 0 ]
        then
            quit "Failed to create $SPARK_EVENTLOG_DIR. Please make sure the user has right to access $SPARK_EVENTLOG_DIR"
        fi
    fi

    SPARK_HISTORYLOG_DIR=`bash $KYLIN_HOME/bin/get-properties.sh kylin.engine.spark-conf.spark.history.fs.logDirectory`
    if [ -n "$SPARK_HISTORYLOG_DIR" ]
    then
        hadoop ${hadoop_conf_param} fs -mkdir -p $SPARK_HISTORYLOG_DIR
        if [ $? != 0 ]
        then
            quit "Failed to create $SPARK_HISTORYLOG_DIR. Please make sure the user has right to access $SPARK_HISTORYLOG_DIR"
        fi
    fi

    if [ ! -d "$spark_home/jars" ]
      then
          echo `setColor 33 "Optional dependency spark not found, if you need this; set SPARK_HOME, or run bin/download-spark.sh"`
          echo "echo 'skip spark_dependency'" > ${dir}/cached-spark-dependency.sh
      else
        spark_dependency=`find -L $spark_home/jars -name '*.jar' ! -name '*slf4j*' ! -name '*calcite*' ! -name '*doc*' ! -name '*test*' ! -name '*sources*' ''-printf '%p:' | sed 's/:$//'`
        if [ -z "$spark_dependency" ]
        then
            quit "spark jars not found"
        else
            verbose "spark dependency: $spark_dependency"
            export spark_dependency
        fi
        echo "export spark_dependency=$spark_dependency" > ${dir}/cached-spark-dependency.sh
    fi
fi
