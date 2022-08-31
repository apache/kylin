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

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

influxdb_version="1.6.4.x86_64"
influxdb_pkg_md5="4c73908a1fc897c5ab010c5fb1852e3e"

if [[ ! -d ${KYLIN_HOME}/influxdb ]]; then
    mkdir -p ${KYLIN_HOME}/influxdb
fi

if [ ! -f "${KYLIN_HOME}/influxdb/influxdb-${influxdb_version}.rpm" ]
then
    echo "No binary file found."
    wget --directory-prefix=${KYLIN_HOME}/influxdb/ https://repos.influxdata.com/centos/6/x86_64/stable/influxdb-${influxdb_version}.rpm && echo "Download InfluxDB success."  || { echo "Download InfluxDB failed." && exit 1; }
else
    if [ `calMd5 ${KYLIN_HOME}/influxdb/influxdb-${influxdb_version}.rpm | awk '{print $1}'` != "${influxdb_pkg_md5}" ]
    then
        echo "md5 check failed."
        rm ${KYLIN_HOME}/influxdb/influxdb-${influxdb_version}.rpm
        wget --directory-prefix=${KYLIN_HOME}/influxdb/ https://repos.influxdata.com/centos/6/x86_64/stable/influxdb-${influxdb_version}.rpm && echo "Download InfluxDB success." || { echo "Download InfluxDB failed." && exit 1; }
    fi
fi
