#!/usr/bin/env bash

current_dir=`pwd`
cd ${current_dir}/build/tomcat/webapps
unzip kylin.war && rm -f kylin.war
cd WEB-INF/lib
#remove slf4j-api-1.7.21.jar to solve slf4j conflict
rm -f slf4j-api-1.7.21.jar
mkdir modify_avatica_jar && mv avatica-1.12.0.jar modify_avatica_jar
cd modify_avatica_jar
#remove org/slf4j in avatica-1.12.0.jar and repackage it to solve slf4j conflict
unzip avatica-1.12.0.jar && rm -f avatica-1.12.0.jar
rm -rf org/slf4j && jar -cf avatica-1.12.0.jar ./
rm -rf `ls | egrep -v avatica-1.12.0.jar`
mv avatica-1.12.0.jar ..
cd .. && rm -rf modify_avatica_jar
cd ${current_dir}/build/tomcat/webapps
#repackage kylin.war
jar -cf kylin.war ./ && rm -rf `ls | egrep -v kylin.war`
cd ${current_dir}