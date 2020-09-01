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

function help_func {
    echo "Usage: kill-process-tree.sh <PID>(process id)"
    echo "       kill-process-tree.sh 12345"
    exit 1
}

function isRunning() {
    [[ -n "$(ps -p $1 -o pid=)" ]]
}

function killTree() {
    local parent=$1 child
    for child in $(ps ax -o ppid= -o pid= | awk "\$1==$parent {print \$2}"); do
        killTree ${child}
    done
    kill ${parent}
    if isRunning ${parent}; then
        sleep 5
        if isRunning ${parent}; then
            kill -9 ${parent}
        fi
    fi
}

# Check parameters count.
if [[ $# -ne 1 ]]; then
    help_func
fi

# Check whether it contains non-digit characters.
# Remove all digit characters and check for length.
# If there's length it's not a number.
if [[ -n ${1//[0-9]/} ]]; then
    help_func
fi

killTree $@