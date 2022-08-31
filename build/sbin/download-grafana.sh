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

grafana_version="6.2.4"
grafana_pkg_md5="b0c5b1085db30914e128980b5fe5f553"

if [[ -d "${KYLIN_HOME}/grafana" ]]; then
    echo "Grafana already exists, please check."
    exit 1
fi

if [ ! -f "${KYLIN_HOME}/grafana-${grafana_version}.linux-amd64.tar.gz" ]
then
    echo "no binary file found "
    wget --directory-prefix=${KYLIN_HOME}/ https://dl.grafana.com/oss/release/grafana-${grafana_version}.linux-amd64.tar.gz && echo "Download grafana success." || echo "Download grafana failed"
else
    if [ `calMd5 ${KYLIN_HOME}/grafana-${grafana_version}.linux-amd64.tar.gz | awk '{print $1}'` != "${grafana_pkg_md5}" ]
    then
        echo "md5 check failed"
        rm ${KYLIN_HOME}/grafana-${grafana_version}.linux-amd64.tar.gz
        wget --directory-prefix=${KYLIN_HOME}/ https://dl.grafana.com/oss/release/grafana-${grafana_version}.linux-amd64.tar.gz && echo "Download grafana success."  || echo "Download grafana failed"
    fi
fi

tar -zxf ${KYLIN_HOME}/grafana-${grafana_version}.linux-amd64.tar.gz -C ${KYLIN_HOME}/ || { exit 1; }
mv ${KYLIN_HOME}/grafana-${grafana_version} ${KYLIN_HOME}/grafana

# remove package
rm ${KYLIN_HOME}/grafana-${grafana_version}.linux-amd64.tar.gz

# change access for grafana
find ${KYLIN_HOME}/grafana -type f -exec chmod 755 {} \;

# check the access
test -d '${KYLIN_HOME}/grafana/conf/provisioning' && rm -rf ${KYLIN_HOME}/grafana/conf/provisioning
test -d '${KYLIN_HOME}/grafana/public/test' && rm -rf ${KYLIN_HOME}/grafana/public/test


cp -rf ${KYLIN_HOME}/tool/grafana/dashboards   ${KYLIN_HOME}/grafana/
cp -rf ${KYLIN_HOME}/tool/grafana/provisioning ${KYLIN_HOME}/grafana/conf/
cp -rf ${KYLIN_HOME}/tool/grafana/custom.ini   ${KYLIN_HOME}/grafana/conf/
