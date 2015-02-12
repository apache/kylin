#!/bin/sh

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
# Copied file becomes 000 for some env (e.g. my Cygwin)
chmod 644 tomcat/webapps/kylin.war
chmod 644 lib/kylin-job-${version}.jar
chmod 644 lib/kylin-coprocessor-${version}.jar

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