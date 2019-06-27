#!/usr/bin/env bash

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

current_dir=`pwd`
cd ${current_dir}/build/tomcat/webapps
unzip kylin.war && rm -f kylin.war
cd WEB-INF/lib
#remove slf4j-api-1.7.21.jar to solve slf4j conflict
rm -f slf4j-api-1.7.21.jar
mkdir modify_avatica_jar && mv avatica-1.10.0.jar modify_avatica_jar
cd modify_avatica_jar
#remove org/slf4j in avatica-1.10.0.jar and repackage it to solve slf4j conflict
unzip avatica-1.10.0.jar && rm -f avatica-1.10.0.jar
rm -rf org/slf4j && jar -cf avatica-1.10.0.jar ./
rm -rf `ls | egrep -v avatica-1.10.0.jar`
mv avatica-1.10.0.jar ..
cd .. && rm -rf modify_avatica_jar
cd ${current_dir}/build/tomcat/webapps
#repackage kylin.war
jar -cf kylin.war ./ && rm -rf `ls | egrep -v kylin.war`
cd ${current_dir}