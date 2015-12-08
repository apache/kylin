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

if [ $# != 1 ]
then
    echo 'invalid input'
    exit -1
fi

cd $KYLIN_HOME/logs

for pidfile in `find . -name "$1_1*"`
do
    pidfile=`echo "$pidfile" | cut -c 3-`
    echo "pidfile:$pidfile"
    pid=`cat $pidfile`
    if [ `ps -ef | awk '{print $2}' | grep -w $pid | wc -l` = 1 ]
    then
        echo "pid:$pid still running"
    else
        echo "pid:$pid not running, try to delete files"
        echo $pidfile | xargs rm
        echo "streaming_$pidfile.log" | xargs rm
    fi
done

