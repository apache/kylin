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

if [ -z "$version" ]
then
    echo 'version not set'
    version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version |  grep -E '^[0-9]+\.[0-9]+\.[0-9]+' `
fi
echo "version ${version}"
export version

sh build/script/prepare-libs.sh || { exit 1; }

cp server/target/kylin-server-${version}.war build/tomcat/webapps/kylin.war
chmod 644 build/tomcat/webapps/kylin.war

echo "add js css to war"
if [ ! -d "webapp/dist" ]
then
    echo "error generate js files"
    exit 1
fi

cd webapp/dist
for f in * .[!.]*
do
    echo "Adding $f to war"
    jar -uf ../../build/tomcat/webapps/kylin.war $f
done
