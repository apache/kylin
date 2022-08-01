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

dir=$(dirname ${0})
cd ${dir}/../..

source build/apache_release/functions.sh

rm -rf build/grafana

grafana_version="6.2.4"
grafana_pkg_md5="b0c5b1085db30914e128980b5fe5f553"

if [ ! -f "build/grafana-${grafana_version}.linux-amd64.tar.gz" ]
then
    echo "no binary file found "
    wget --directory-prefix=build/ https://dl.grafana.com/oss/release/grafana-${grafana_version}.linux-amd64.tar.gz || echo "Download grafana failed"
else
    if [ `calMd5 build/grafana-${grafana_version}.linux-amd64.tar.gz | awk '{print $1}'` != "${grafana_pkg_md5}" ]
    then
        echo "md5 check failed"
        rm build/grafana-${grafana_version}.linux-amd64.tar.gz
        wget --directory-prefix=build/ https://dl.grafana.com/oss/release/grafana-${grafana_version}.linux-amd64.tar.gz || echo "Download grafana failed"
    fi
fi

tar -zxf build/grafana-${grafana_version}.linux-amd64.tar.gz -C build/ || { exit 1; }
mv build/grafana-${grafana_version} build/grafana

test -d 'build/grafana/conf/provisioning' && rm -rf build/grafana/conf/provisioning

test -d 'build/grafana/public/test' && rm -rf build/grafana/public/test

cp -rf build/deploy/grafana/dashboards build/grafana/
cp -rf build/deploy/grafana/provisioning build/grafana/conf/
cp -rf build/deploy/grafana/custom.ini build/grafana/conf/
