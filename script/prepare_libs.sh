#!/bin/bash

if [ -z "$version" ]
then
    echo 'version not set'
    version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\['`
fi
echo "version ${version}"

echo "copy lib file"
rm -rf lib
mkdir lib
cp job/target/kylin-job-${version}-job.jar lib/kylin-job-${version}.jar
cp storage/target/kylin-storage-${version}-coprocessor.jar lib/kylin-coprocessor-${version}.jar
cp jdbc/target/kylin-jdbc-${version}.jar lib/kylin-jdbc-${version}.jar
# Copied file becomes 000 for some env (e.g. my Cygwin)
chmod 644 lib/kylin-job-${version}.jar
chmod 644 lib/kylin-coprocessor-${version}.jar
chmod 644 lib/kylin-jdbc-${version}.jar