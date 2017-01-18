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

rm -rf build/tomcat

alias md5cmd="md5sum"
if [[ `uname -a` =~ "Darwin" ]]; then
    alias md5cmd="md5 -q"
fi

tomcat_pkg_version="8.5.9"
tomcat_pkg_md5="b41270a64b7774c964e4bec813eea2ed"

if [ ! -f "build/apache-tomcat-${tomcat_pkg_version}.tar.gz" ]
then
    echo "no binary file found"
    wget --directory-prefix=build/ http://archive.apache.org/dist/tomcat/tomcat-8/v${tomcat_pkg_version}/bin/apache-tomcat-${tomcat_pkg_version}.tar.gz || echo "Download tomcat failed"
else
    if [ `md5cmd build/apache-tomcat-${tomcat_pkg_version}.tar.gz | awk '{print $1}'` != "${tomcat_pkg_md5}" ]
    then
        echo "md5 check failed"
        rm build/apache-tomcat-${tomcat_pkg_version}.tar.gz
        wget --directory-prefix=build/ http://archive.apache.org/dist/tomcat/tomcat-8/v${tomcat_pkg_version}/bin/apache-tomcat-${tomcat_pkg_version}.tar.gz || echo "download tomcat failed"
    fi
fi
unalias md5cmd

tar -zxvf build/apache-tomcat-${tomcat_pkg_version}.tar.gz -C build/
mv build/apache-tomcat-${tomcat_pkg_version} build/tomcat
rm -rf build/tomcat/webapps/*

mv build/tomcat/conf/server.xml build/tomcat/conf/server.xml.bak
mv build/tomcat/conf/context.xml build/tomcat/conf/context.xml.bak
cp build/deploy/server.xml build/tomcat/conf/server.xml
echo "server.xml overwritten..."
cp build/deploy/context.xml build/tomcat/conf/context.xml
echo "context.xml overwritten..."


if [ -z "$version" ]
then
    echo 'version not set'
    version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\['`
fi
echo "version ${version}"
export version

cp tomcat-ext/target/kylin-tomcat-ext-${version}.jar build/tomcat/lib/kylin-tomcat-ext-${version}.jar
chmod 644 build/tomcat/lib/kylin-tomcat-ext-${version}.jar

