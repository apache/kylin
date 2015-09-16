#!/bin/bash

echo "package back-end"

dir=$(dirname ${0})
cd ${dir}/../..

mvn clean install -DskipTests	 || { exit 1; }

#package webapp
echo 'package front-end'
cd webapp
npm install -g bower			 || { exit 1; }
bower --allow-root install		 || { exit 1; }
npm install						 || { exit 1; }
npm install -g grunt-cli		 || { exit 1; }
grunt dev --buildEnv=dev		 || { exit 1; }
