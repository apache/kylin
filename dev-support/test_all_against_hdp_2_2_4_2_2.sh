#!/bin/bash

dir=$(dirname ${0})
cd ${dir}
cd ..

mvn clean install -DskipTests | tee mci.log
mvn test -Dhdp.version=2.2.4.2-2 -Dtest=org.apache.kylin.job.BuildCubeWithEngineTest -DfailIfNoTests=false -P sandbox | tee BuildCubeWithEngineTest.log
mvn test -Dhdp.version=2.2.4.2-2 -Dtest=org.apache.kylin.job.BuildIIWithStreamTest -DfailIfNoTests=false -P sandbox | tee BuildIIWithStreamTest.log
mvn test -Dhdp.version=2.2.4.2-2 -Dtest=org.apache.kylin.job.BuildCubeWithStreamTest -DfailIfNoTests=false -P sandbox | tee BuildCubeWithStreamTest.log
mvn verify -Dhdp.version=2.2.4.2-2 -fae -P sandbox | tee mvnverify.log