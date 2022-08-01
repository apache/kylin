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

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh $@

function checkFileOccupied() {
    target_file=$1
    pids="`fuser $target_file 2>&1`"
    if [[ "${pids}" == "" ]]; then
        echo false
    else
        echo true
    fi
}

function checkSizeExceedLimit() {
    target_file=$1
    file_threshold=`${KYLIN_HOME}/bin/get-properties.sh kylin.env.max-keep-log-file-threshold-mb`
    file_size=`du -b "$target_file" | cut -f 1`
    let file_threshold=file_threshold*1024*1024
    if [[ ${file_size} -gt ${file_threshold} ]]; then
        echo true
    else
        echo false
    fi
}

function logRotate() {
    target_file=$1
    # keep 10 history log files
    keep_limit=`${KYLIN_HOME}/bin/get-properties.sh kylin.env.max-keep-log-file-number`

    is_occupied=`checkFileOccupied ${target_file}`
    if [[ "${is_occupied}" == "true" ]]; then
        return
    fi

    is_too_large=`checkSizeExceedLimit ${target_file}`
    if [[ "${is_too_large}" == "false" ]]; then
        return
    fi

    if [[ -f $target_file ]]; then
        if [[ -f ${target_file}.${keep_limit} ]]; then
            # clean oldest log file first
            rm -f ${target_file}.${keep_limit}
        fi

        let p_cnt=keep_limit-1
        # renames logs .1 trough .${keep_limit}
        while [[ $keep_limit -gt 1 ]]; do
            if [ -f ${target_file}.${p_cnt} ] ; then
                mv -f ${target_file}.${p_cnt} ${target_file}.${keep_limit}
            fi
            let keep_limit=keep_limit-1
            let p_cnt=p_cnt-1
        done

        # rename current log to .1
        mv -f $target_file $target_file.1
    fi
}

ERR_LOG=${KYLIN_HOME}/logs/shell.stderr
OUT_LOG=${KYLIN_HOME}/logs/shell.stdout
KYLIN_OUT=${KYLIN_HOME}/logs/kylin.out

if [ "$1" == "start" ] || [ "$1" == "spawn" ]
then
    logRotate $ERR_LOG
    logRotate $OUT_LOG
    logRotate $KYLIN_OUT
fi