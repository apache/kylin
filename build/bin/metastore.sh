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

RESTORE='\033[0m'
YELLOW='\033[00;33m'

if [ -z $KYLIN_HOME ];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

function help {
    echo "usage: metastore.sh backup METADATA_BACKUP_PATH(the default path is KYLIN_HOME/meta_backups/)"
    echo "       metastore.sh fetch TARGET_FILE_PATH METADATA_FETCH_PATH(the default path is KYLIN_HOME/meta_fetch/)"
    echo "       metastore.sh list TARGET_FOLDER_PATH"
    echo "       metastore.sh restore METADATA_RESTORE_PATH [--after-truncate]"
    echo "       metastore.sh backup-project PROJECT_NAME METADATA_BACKUP_PATH(the default path is KYLIN_HOME/meta_backups/)"
    echo "       metastore.sh restore-project PROJECT_NAME METADATA_RESTORE_PATH [--after-truncate]"
    exit 1
}

function printBackupResult() {
    error=$1
    if [[ $error == 0 ]]; then
        if [[ -z "${path}" ]]; then
            path="\${KYLIN_HOME}/meta_backups"
        fi
        echo -e "${YELLOW}Backup at local disk succeed.${RESTORE}"
    else
        echo -e "${YELLOW}Backup failed. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
    fi
}

function printFetchResult() {
  error=$1
  if [[ $error == 0 ]]; then
      if [[ -z "$path" ]]; then
          path="\${KYLIN_HOME}/meta_fetch"
      fi
      echo -e "${YELLOW} Fetch at local dist succeed.${RESTORE}"
  else
      echo -e "${YELLOW} Fetch failed.${RESTORE}"
  fi
}

function printRestoreResult() {
    error=$1

    if [[ $error == 0 ]]; then
        echo -e "${YELLOW}Restore succeed. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
    else
        echo -e "${YELLOW}Restore failed. Detailed Message is at \"logs/shell.stderr\".${RESTORE}"
    fi
}

function check_path_empty() {
  # this function is to check whether the path is an empty str.
    if [ -z "$1" ]; then
          exit 1
    fi
}

function check_path_empty_dir() {
  # this function is to check whether the path is an empty directory.
    if [ -z "$(ls -A "$1")" ]; then
        echo -e "${YELLOW}The directory \"$1\" is an empty directory, please check.${RESTORE}"
        exit 1
    fi
}

function restore_all() {
        local path=`cd $1 && pwd -P`
        check_path_empty ${path}
        check_path_empty_dir ${path}
        ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.MetadataTool -restore -dir ${path} ${2}
        printRestoreResult $?
}

function restore_project() {
        local path=`cd $1 && pwd -P`
        check_path_empty ${path}
        check_path_empty_dir ${path}
        ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.MetadataTool -restore -dir ${path} -project ${2} ${3}
        printRestoreResult $?
}


if [ "$1" == "backup" ]
then
    BACKUP_OPTS="-backup"
    if [ $# -eq 2 ]; then
        path=`cd $2 && pwd -P`
        check_path_empty ${path}
        BACKUP_OPTS="${BACKUP_OPTS} -dir ${path}"
    elif [ $# -ne 1 ]; then
        help
    fi

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.MetadataTool ${BACKUP_OPTS}
    printBackupResult $?

elif [ "$1" == "fetch" ]
then
    FETCH_OPTS="-fetch"
    if [ $# -eq 2 ]; then
        _file=$2
        FETCH_OPTS="${FETCH_OPTS} -target ${_file}"
    elif [ $# -eq 3 ]; then
        _file=$2
        path=`cd $3 && pwd -P`
        check_path_empty ${path}
        FETCH_OPTS="${FETCH_OPTS} -target ${_file} -dir ${path}"
    else
        help
    fi

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.MetadataTool ${FETCH_OPTS}
    printFetchResult $?

elif [ "$1" == "list" ]
then
    if [ $# -eq 2 ]; then
        _folder=$2
        LIST_OPTS="-list -target ${_folder}"
    else
        help
    fi
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.MetadataTool ${LIST_OPTS}

elif [ "$1" == "restore" ]
then
    if [ $# -eq 2 ]; then
        restore_all ${2}
    elif [ $# -eq 3 -a "$3" == "--after-truncate" ]; then
        restore_all ${2} "--after-truncate"
    else
       help
    fi

elif [ "$1" == "backup-project" ]
then
    BACKUP_OPTS="-backup"
    if [ $# -eq 3 ]; then
        path=`cd $3 && pwd -P`
        check_path_empty ${path}
        BACKUP_OPTS="${BACKUP_OPTS} -dir ${path}"
    elif [ $# -ne 2 ]; then
        help
    fi
    BACKUP_OPTS="${BACKUP_OPTS} -project $2"
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.MetadataTool ${BACKUP_OPTS}
    printBackupResult $?

elif [ "$1" == "restore-project" ]
then
    if [ $# -eq 3 ]; then
        restore_project ${3} ${2}
    elif [ $# -eq 4 -a "$4" == "--after-truncate" ]; then
        restore_project ${3} ${2} "--after-truncate"
    else
        help
    fi
else
    help
fi

