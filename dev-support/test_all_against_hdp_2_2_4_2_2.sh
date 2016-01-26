#!/bin/bash

dir=$(dirname ${0})
cd ${dir}
cd ..

mvn clean install -DskipTests | tee mci.log
mvn verify -Dhdp.version=2.2.4.2-2 -fae | tee mvnverify.log
