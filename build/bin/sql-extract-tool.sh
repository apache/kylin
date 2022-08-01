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

function verbose() {
    (>&2 echo `date '+%F %H:%M:%S'` $@)
}

function help() {
    echo "Example usage:"
    echo "  sql-extract-tool.sh -file <LOG_FILE_PATH> "
    exit 1
}

function extract_sql() {
    log_file=$2
    log_file_dir=`dirname ${log_file}`
    sql_file=${log_file_dir}/kylin_sql_`date '+%F_%H:%M:%S'`.txt
    sql_file_tmp=${sql_file}.tmp
    if [ -f ${sql_file} ];then
        rm -rf ${sql_file}
    fi

    if [ -f ${sql_file_tmp} ];then
        rm -rf ${sql_file_tmp}
    fi

    verbose "start to extract sql from [${log_file}] to [${sql_file}]"

    sql=""
    sql_start=0
    sql_end=0
    while read line
    do

    if [[ $line == SQL:* ]];then
        sql_start=1
        sql=${line:4}
    elif [[ $line == User:* ]];then
        sql_end=1
    elif [ $sql_start == 1 ] && [ $sql_end == 0 ];then
        sql="$sql $line"
    elif [[ $line == "Success: true" ]];then
        echo "$sql;" >> ${sql_file_tmp}
        sql_start=0
        sql_end=0
        sql=""
    else
        sql_start=0
        sql_end=0
        sql=""
    fi
    done < ${log_file}

    row_count=$(cat ${log_file} | wc -l)
    verbose "file [${log_file}] total scan row count : ${row_count}"

    sort -u ${sql_file_tmp} >> ${sql_file}

    sql_count=$(cat ${sql_file} | wc -l)
    verbose "file [${log_file}] total extract sql num : ${sql_count}"

    rm -rf ${sql_file_tmp}
    sql_file_size=$(ls -lah ${sql_file} | awk '{ print $5}')
    verbose "extract sql succeed, the sql file size is ${sql_file_size}"
    split -d -C 4M ${sql_file} "${sql_file}-"
}

function main() {
    if [[ $# -lt 2 ]]; then
        help
    fi

    if [[ $1 == "-file" ]]; then
        extract_sql $@
        exit $?
    else
        help
    fi
}

main $@
