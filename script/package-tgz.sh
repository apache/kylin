#!/bin/sh

# pack webapp into kylin.war so that we have a all-in-one war
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
cp server/target/kylin-server-${version}.war job/target/kylin-job-${version}-job.jar storage/target/kylin-storage-${version}-coprocessor.jar lib

echo "add js css to war"
cd webapp/dist
for f in * .[^.]*
do
    echo "Adding $f to war"
    jar -uf ../../lib/kylin-server-${version}.war $f
done
cd ../..

#package tgz
echo 'package tgz'
tar -zcvf kylin-${version}.tgz lib bin conf
