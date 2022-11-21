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

mvn versions:set -DnewVersion=${RELEASE_VERSION}
RELEASE_VERSION=$(mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec)

mvn clean install -e -DskipTests -pl src/jdbc

if [[ -d jdbc_dist ]]; then
    rm -rf jdbc_dist/*
fi
mkdir -p jdbc_dist

cp src/jdbc/target/kylin-*.jar jdbc_dist/
cp src/jdbc/target/kylin-jdbc.properties jdbc_dist/
cp src/jdbc/libs/*.jar jdbc_dist/
rm -f jdbc_dist/*-tests.jar
cd jdbc_dist
tar -zcvf kylin-jdbc-${RELEASE_VERSION}.tar.gz ./*
rm *.jar
rm *.properties

echo "Packaging JDBC is done, please check the directory: `pwd`."
