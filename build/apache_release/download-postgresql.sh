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

mkdir -p build/postgresql


pg_urls=(
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/postgresql10-libs-10.7-1PGDG.rhel6.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/postgresql10-10.7-1PGDG.rhel6.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/postgresql10-server-10.7-1PGDG.rhel6.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-7-x86_64/postgresql10-libs-10.7-1PGDG.rhel7.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-7-x86_64/postgresql10-10.7-1PGDG.rhel7.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-7-x86_64/postgresql10-server-10.7-1PGDG.rhel7.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-8-x86_64/postgresql10-libs-10.8-1PGDG.rhel8.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-8-x86_64/postgresql10-10.8-1PGDG.rhel8.x86_64.rpm"
    "https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-8-x86_64/postgresql10-server-10.8-1PGDG.rhel8.x86_64.rpm"
)

pg_file_md5=(
    "28f75cae7734a0f62cb6ae774d2a1378"
    "0b681ba97a9ddc478f820d2287de3b2d"
    "34cb34bcaf5388c96ffc758da4ed8a2a"
    "648a350f2421a8320cebe40a812909e9"
    "62a2915053f5f2ffd5ef1bbbc106e7be"
    "5ef931ddedeca1c81a998763292aafad"
    "23cea91ae23d4f30802d2d3f5b47eac4"
    "dae27bb23602348d593a731968f23331"
    "a5f563d7bf7fd0444250bb4bba6f3d4d"
)

for ((i=0;i<${#pg_urls[@]};i++))
do
    url=${pg_urls[$i]}
    file_name=${url##*/}
    if [ ! -f "build/postgresql/$file_name" ]
    then
        echo "No binary file found "
        wget --no-check-certificate --directory-prefix=build/postgresql/ $url || { echo "Download $file_name failed." && exit 1; }
    else
        if [ `calMd5 build/postgresql/$file_name | awk '{print $1}'` != "${pg_file_md5[$i]}" ]
        then
            echo "md5 check failed"
            rm build/$file_name
            wget --no-check-certificate --directory-prefix=build/postgresql/ $url || { echo "Download $file_name failed." && exit 1; }
        fi
    fi
done



#cp build/deploy/licenses/postgresql-license build/postgresql/LICENSE || { echo "No license for PostgreSQL found, please check build/deploy/licenses" && exit 1; }
