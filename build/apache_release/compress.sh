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
exportProjectVersions

# get package name
current_branch=${branch}
if [[ "${current_branch}" = "" ]]; then
    current_branch=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p')
fi

# package as *.tar.gz
echo "package name: ${package_name}"
cd build/
rm -rf ${package_name}
mkdir ${package_name}

cp -rf VERSION commit_SHA1 lib ${package_name}/

mkdir -p ${package_name}/lib/ext

cp -rf spark ${package_name}/
cp -rf sample_project ${package_name}/
cp -rf samples ${package_name}/
cp -rf influxdb ${package_name}/
cp -rf grafana ${package_name}/
cp -rf postgresql ${package_name}/

# Add ssb data preparation files
mkdir -p ${package_name}/tool/ssb
cp -rf ../src/examples/sample_cube/data ${package_name}/tool/ssb/
cp -rf ../src/examples/sample_cube/create_sample_ssb_tables.sql ${package_name}/tool/ssb/

# Add ops_plan files
cp -rf ../ops_plan ${package_name}/

# Add conf profiles
mkdir -p ${package_name}/conf
mkdir -p ${package_name}/tool/conf
mkdir -p ${package_name}/server/conf
for log_conf in `find conf -name "*-log4j.xml"`; do
    cp ${log_conf} ${package_name}/${log_conf}.template
    if [[ ${log_conf} == *"tool"* ]]; then
        cp ${log_conf} ${package_name}/tool/${log_conf}
    else
        cp ${log_conf} ${package_name}/server/${log_conf}
    fi
done
cp -rf conf/kylin.properties ${package_name}/conf/kylin.properties
cp -rf conf/setenv.sh ${package_name}/conf/setenv.sh.template
cp -rf bin/ ${package_name}/bin/
cp -rf sbin/ ${package_name}/sbin/

rm -rf ext lib commit_SHA1 VERSION # keep the spark folder on purpose

cp -rf server/webapp/dist ${package_name}/server/public
cp -rf server/newten.jar ${package_name}/server/
cp -rf server/jars ${package_name}/server/
cp -rf deploy/.keystore ${package_name}/server/
mv ${package_name}/server/jars/log4j* ${package_name}/spark/jars/
rm -rf server/

## comment all default properties, and append them to the user visible kylin.properties
## first 16 lines are license, just skip them
sed '1,21d' ../src/core-common/src/main/resources/kylin-defaults0.properties | awk '{print "#"$0}' >> ${package_name}/conf/kylin.properties

find ${package_name} -type d -exec chmod 755 {} \;
find ${package_name} -type f -exec chmod 644 {} \;
find ${package_name} -type f -name "*.sh" -exec chmod 755 {} \;
find ${package_name}/spark -type f -exec chmod 755 {} \;
find ${package_name}/influxdb -type f -exec chmod 755 {} \;
find ${package_name}/grafana -type f -exec chmod 755 {} \;
find ${package_name}/postgresql -type f -exec chmod 755 {} \;

rm -rf ../dist
mkdir -p ../dist

tar -czf ../dist/${package_name}.tar.gz ${package_name}
rm -rf ${package_name}

echo "Package ready."
cd ../dist
ls ${package_name}*.tar.gz
