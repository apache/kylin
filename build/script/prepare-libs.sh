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

echo "copy lib file"
rm -rf build/lib build/tool
mkdir build/lib build/tool
cp assembly/target/kylin-assembly-${version}-job.jar build/lib/kylin-job-${version}.jar
cp storage-hbase/target/kylin-storage-hbase-${version}-coprocessor.jar build/lib/kylin-coprocessor-${version}.jar
cp jdbc/target/kylin-jdbc-${version}.jar build/lib/kylin-jdbc-${version}.jar
cp tool-assembly/target/kylin-tool-assembly-${version}-assembly.jar build/tool/kylin-tool-${version}.jar
cp datasource-sdk/target/kylin-datasource-sdk-${version}-lib.jar build/lib/kylin-datasource-sdk-${version}.jar

# Copied file becomes 000 for some env (e.g. my Cygwin)
chmod 644 build/lib/kylin-job-${version}.jar
chmod 644 build/lib/kylin-coprocessor-${version}.jar
chmod 644 build/lib/kylin-jdbc-${version}.jar
chmod 644 build/tool/kylin-tool-${version}.jar
chmod 644 build/lib/kylin-datasource-sdk-${version}.jar
