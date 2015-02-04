#!/bin/sh

echo "package back-end"

dir=$(dirname ${0})
cd ${dir}/..

mvn clean install -DskipTests
