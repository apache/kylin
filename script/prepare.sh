#!/bin/sh
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
cd ${dir}/..

if [ -z "$version" ]
then
    echo 'version not set'
    version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\['`
fi
echo "version ${version}"

echo "copy lib file"
rm -rf lib
mkdir lib
cp server/target/kylin-server-${version}.war tomcat/webapps/kylin.war
cp job/target/kylin-job-${version}-job.jar lib/kylin-job-${version}.jar
cp storage/target/kylin-storage-${version}-coprocessor.jar lib/kylin-coprocessor-${version}.jar
cp jdbc/target/kylin-jdbc-${version}.jar lib/kylin-jdbc-${version}.jar
cp monitor/target/kylin-monitor-${version}.jar lib/kylin-monitor-${version}.jar
# Copied file becomes 000 for some env (e.g. my Cygwin)
chmod 644 tomcat/webapps/kylin.war
chmod 644 lib/kylin-job-${version}.jar
chmod 644 lib/kylin-coprocessor-${version}.jar
chmod 644 lib/kylin-jdbc-${version}.jar
chmod 644 lib/kylin-monitor-${version}.jar

echo "add js css to war"
if [ ! -d "webapp/dist" ]
then
    echo "error generate js files"
    exit 1
fi

cd webapp/dist
for f in * .[^.]*
do
    echo "Adding $f to war"
    jar -uf ../../tomcat/webapps/kylin.war $f
done