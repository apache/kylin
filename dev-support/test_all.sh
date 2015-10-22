#!/bin/bash

dir=$(dirname ${0})
cd ${dir}
cd ..

mvn clean install -DskipTests | tee mci.log
mvn test -Dtest=org.apache.kylin.job.BuildCubeWithEngineTest -DfailIfNoTests=false -P sandbox | tee BuildCubeWithEngineTest.log
mvn test -Dtest=org.apache.kylin.job.BuildIIWithStreamTest -DfailIfNoTests=false -P sandbox | tee BuildIIWithStreamTest.log
mvn test -Dtest=org.apache.kylin.job.BuildCubeWithStreamTest -DfailIfNoTests=false -P sandbox | tee BuildCubeWithStreamTest.log
mvn verify -fae -P sandbox | tee mvnverify.log