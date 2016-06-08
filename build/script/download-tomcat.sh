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

if [ ! -f "build/apache-tomcat-7.0.69.tar.gz" ]
then
    echo "no binary file found"
    wget --directory-prefix=build/ http://archive.apache.org/dist/tomcat/tomcat-7/v7.0.69/bin/apache-tomcat-7.0.69.tar.gz || echo "download tomcat failed"
else
    if [ `md5cmd build/apache-tomcat-7.0.69.tar.gz | awk '{print $1}'` != "10a071e5169a1a8b14ff35a0ad181052" ]
    then
        echo "md5 check failed"
        rm build/apache-tomcat-7.0.69.tar.gz
        wget --directory-prefix=build/ http://archive.apache.org/dist/tomcat/tomcat-7/v7.0.69/bin/apache-tomcat-7.0.69.tar.gz || echo "download tomcat failed"
    fi
fi
unalias md5cmd

tar -zxvf build/apache-tomcat-7.0.69.tar.gz -C build/
mv build/apache-tomcat-7.0.69 build/tomcat
rm -rf build/tomcat/webapps/*

mv build/tomcat/conf/server.xml build/tomcat/conf/server.xml.bak
cp build/deploy/server.xml build/tomcat/conf/server.xml
echo "server.xml overwritten..."
