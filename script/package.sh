#!/bin/bash

echo "Checking maven..."

if [ -z "$(command -v mvn)" ]
then
    echo "Please install maven first so that Kylin packaging can proceed"
    exit 1
else
    echo "maven check passed"
fi

if [ -z "$(command -v git)" ]
then
    echo "Please install maven first so that Kylin packaging can proceed"
    exit 1
else
    echo "git check passed"
fi


echo "Checking npm..."

if [ -z "$(command -v npm)" ]
then
    echo "Please install npm first so that Kylin packaging can proceed"
    exit 1
else
    echo "npm check passed"
fi

dir=$(dirname ${0})
cd ${dir}/..
version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\['`
echo "kylin version: ${version}"
export version

#commit id
git rev-parse HEAD > commit_SHA1

sh script/download-tomcat.sh || { exit 1; }
sh script/build.sh || { exit 1; }
sh script/prepare.sh || { exit 1; }
sh script/compress.sh || { exit 1; }

