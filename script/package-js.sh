#!/bin/sh

#package webapp
echo 'package front-end'
dir=$(dirname ${0})
cd ${dir}/../webapp
npm install
npm install -g grunt-cli
grunt dev --buildEnv=dev
