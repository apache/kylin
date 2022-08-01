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

if [[ -z $KYLIN_HOME ]];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

function help {
    echo "Usage: 'admin-tool.sh admin-password-reset'"
    return 1
}

function printAdminPasswordResetResult() {
    error=$1

    if [[ $error != 0 ]]; then
        echo -e "${YELLOW}Reset the ADMIN password failed, for more details please refer to \"\$KYLIN_HOME/logs/shell.stderr\".${RESTORE}"
    fi
}

ret=0
if [[ "$1" == "admin-password-reset" ]]; then
    read -p "You are resetting the password of [ADMIN], please enter [Y/N] to continue.: " input
    if [[ ${input} != [Yy] ]]; then
        echo "Reset password of [ADMIN] failed."
        exit 1
    fi

    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.security.KapPasswordResetCLI
    ret=$?
    printAdminPasswordResetResult ${ret}
else
    help
    ret=$?
fi

exit ${ret}
