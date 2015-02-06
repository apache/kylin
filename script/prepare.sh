#!/bin/sh

dir=$(dirname ${0})
cd ${dir}/..

if [ -z "$version" ]
then
    echo 'version not set'
    version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\['`
    echo "${version}"
fi

echo "copy lib file"
rm -rf lib
mkdir lib
cp server/target/kylin-server-${version}.war lib
cp job/target/kylin-job-${version}-job.jar lib/kylin-job-${version}.jar
cp storage/target/kylin-storage-${version}-coprocessor.jar lib/kylin-coprocessor-${version}.jar

echo "add js css to war"
cd webapp/dist
for f in * .[^.]*
do
    echo "Adding $f to war"
    jar -uf ../../lib/kylin-server-${version}.war $f
done