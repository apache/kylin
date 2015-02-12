#!/bin/sh

echo "package back-end"

dir=$(dirname ${0})
cd ${dir}/..

mvn clean install -DskipTests

#package webapp
echo 'package front-end'
cd webapp
npm install -g bower
bower --allow-root install
npm install
npm install -g grunt-cli
grunt dev --buildEnv=dev
