#!/bin/sh

dir=$(dirname ${0})
cd ${dir}/..

if [ -z "$version" ]
then
    echo 'version not set'
    version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\['`
fi
echo "version ${version}"
export version

sh script/prepare_libs.sh || { exit 1; }

cp server/target/kylin-server-${version}.war tomcat/webapps/kylin.war
chmod 644 tomcat/webapps/kylin.war

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