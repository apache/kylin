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

source build/release/functions.sh
exportProjectVersions

# package as *.tar.gz
echo "package name: ${package_name}"
cd build/
rm -rf ${package_name}
mkdir ${package_name}

cp -rf VERSION commit_SHA1 lib ${package_name}/
cp ../LICENSE ${package_name}/
cp ../NOTICE ${package_name}/

mkdir -p ${package_name}/lib/ext

# Spark binary won't be distributed into Kylin binary due to size limit
# cp -rf spark ${package_name}/
cp -rf sample_project ${package_name}/
cp -rf samples ${package_name}/

if [[ -d "influxdb" ]]; then
    cp -rf influxdb ${package_name}/
    cp -rf grafana ${package_name}/
    cp -rf postgresql ${package_name}/
fi

## copy async profiler native files
bash async-profiler-lib/download-async-profiler.sh
#cp -rf async-profiler-lib/libasyncProfiler-mac.so "${package_name}"/lib/libasyncProfiler-mac.so
cp -rf libasyncProfiler-linux-x64.so "${package_name}"/lib/libasyncProfiler-linux-x64.so
cp -rf libasyncProfiler-linux-arm64.so "${package_name}"/lib/libasyncProfiler-linux-arm64.so

# Add ssb data preparation files
mkdir -p ${package_name}/tool/ssb
cp -rf ../src/examples/sample_cube/data ${package_name}/tool/ssb/
cp -rf ../src/examples/sample_cube/create_sample_ssb_tables.sql ${package_name}/tool/ssb/

# Add grafana preparation files
mkdir -p ${package_name}/tool/grafana
cp -rf ../build/deploy/grafana/dashboards   ${package_name}/tool/grafana/
cp -rf ../build/deploy/grafana/provisioning ${package_name}/tool/grafana/
cp -rf ../build/deploy/grafana/custom.ini   ${package_name}/tool/grafana/

# Add JDBC Driver
bash release/jdbc_package.sh
cp ../jdbc_dist/kylin-jdbc-${RELEASE_VERSION}.tar.gz "${package_name}"/lib

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

spark_version_pom=`mvn -f ../pom.xml help:evaluate -Dexpression=spark.version | grep -E '^[0-9]+\.[0-9]+\.[0-9]+' `
echo "Prepare download spark scripts for end user with version ${spark_version_pom}."
sed -i "s/SPARK_VERSION_IN_BINARY/${spark_version_pom}/g" ../build/sbin/download-spark-user.sh

rm -rf ext lib commit_SHA1 VERSION # keep the spark folder on purpose

cp -rf server/webapp/dist ${package_name}/server/public
cp -rf server/newten.jar ${package_name}/server/
cp -rf server/jars ${package_name}/server/
cp -rf deploy/.keystore ${package_name}/server/
# mv ${package_name}/server/jars/log4j* ${package_name}/spark/jars/
rm -rf server/

## comment all default properties, and append them to the user visible kylin.properties
## first 16 lines are license, just skip them
sed '1,21d' ../src/core-common/src/main/resources/kylin-defaults0.properties | awk '{print "#"$0}' >> ${package_name}/conf/kylin.properties

find ${package_name} -type d -exec chmod 755 {} \;
find ${package_name} -type f -exec chmod 644 {} \;
find ${package_name} -type f -name "*.sh" -exec chmod 755 {} \;
# find ${package_name}/spark -type f -exec chmod 755 {} \;

if [[ -d "${package_name}/postgresql" ]]; then
    find ${package_name}/influxdb -type f -exec chmod 755 {} \;
    find ${package_name}/grafana -type f -exec chmod 755 {} \;
    find ${package_name}/postgresql -type f -exec chmod 755 {} \;
fi

rm -rf ../dist
mkdir -p ../dist

tar -czf ../dist/${package_name}.tar.gz ${package_name}
rm -rf ${package_name}

echo "Package ready."
cd ../dist
ls ${package_name}*.tar.gz
