#!/bin/bash

echo "Checking maven..."

if [ -z "$(command -v mvn)" ]
then
    echo "Please install maven first so that Kylin-Deploy can proceed"
    exit 1
else
    echo "maven check passed"
fi

echo "Checking npm..."

if [ -z "$(command -v npm)" ]
then
    echo "Please install npm first so that Kylin-Deploy can proceed"
    exit 1
else
    echo "npm check passed"
fi

version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\['`
echo "kylin version: ${version}"
export version

dir=$(dirname ${0})
echo ${pwd}
echo ${dir}

sh ${dir}/package.sh || { exit 1; }
sh ${dir}/prepare.sh || { exit 1; }
sh ${dir}/download-tomcat.sh || { exit 1; }
sh ${dir}/compress.sh || { exit 1; }

