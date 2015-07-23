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

cd dist
rm -rf tomcat

if [ ! -f "apache-tomcat-7.0.59.tar.gz" ]
then
    echo "not binary file found"
    wget  http://archive.apache.org/dist/tomcat/tomcat-7/v7.0.59/bin/apache-tomcat-7.0.59.tar.gz || echo "download tomcat failed"
else
    if [ `md5sum apache-tomcat-7.0.59.tar.gz | awk '{print $1}'` != "ec570258976edf9a833cd88fd9220909" ]
    then
        echo "md5 check failed"
        rm apache-tomcat-7.0.59.tar.gz
        wget  http://archive.apache.org/dist/tomcat/tomcat-7/v7.0.59/bin/apache-tomcat-7.0.59.tar.gz || echo "download tomcat failed"
    fi
fi

tar -zxvf apache-tomcat-7.0.59.tar.gz

mv apache-tomcat-7.0.59 tomcat
rm -rf tomcat/webapps/*

mv tomcat/conf/server.xml tomcat/conf/server.xml.bak
cp ../deploy/server.xml tomcat/conf/server.xml
echo "server.xml overwritten..."
