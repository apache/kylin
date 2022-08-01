#!/bin/bash

#
# Copyright (C) 2016 Kyligence Inc. All rights reserved.
#
# http://kyligence.io
#
# This software is the confidential and proprietary information of
# Kyligence Inc. ("Confidential Information"). You shall not disclose
# such Confidential Information and shall use it only in accordance
# with the terms of the license agreement you entered into with
# Kyligence Inc.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

#title=Checking ClickHouse Cluster Health

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

source ${KYLIN_HOME}/sbin/init-kerberos.sh
## init Kerberos if needed
initKerberosIfNeeded

echo "Checking ClickHouse Cluster Health..."

SECOND_STORAGE_CLICKHOUSE_ENABLED=`$KYLIN_HOME/bin/get-properties.sh kylin.second-storage.class`

if [[ "${SECOND_STORAGE_CLICKHOUSE_ENABLED}" == "org.apache.kylin.clickhouse.ClickHouseStorage" ]]
then
    output=`${KYLIN_HOME}/sbin/bootstrap.sh 'org.apache.kylin.clickhouse.tool.ClickHouseSanityCheckTool' 10`
    if [[ $? == 0 ]]
    then
        echo "check ClickHouse cluster access succeeded."
        exit 0
    else
        echo "check ClickHouse cluster access failed."
        exit 1
    fi
else
  echo "Second storage is not enabled. Skip second storage cluster check."
  exit 3
fi
