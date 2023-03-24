#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ci_output=ci-results-`date +"%Y-%m-%d"`.txt

mvn -U clean install -T 2C -Dmaven.compile.fork=true -DskipTests
echo "----------- Kylin Install Success -----------"

mvn clean test --fail-at-end -pl src/assembly -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/common-booter -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/common-server -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/common-service -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/core-common -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/core-job -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/core-metadata -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/core-metrics -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/core-storage -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/data-loading-booter -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/data-loading-server -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/data-loading-service -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/datasource-sdk -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/datasource-service -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/distributed-lock-ext -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/jdbc -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/job-service -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/kylin-it -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/metadata-server -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/modeling-service -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/query -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/query-booter -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/query-common -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/query-server -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/query-service -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/server -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/source-hive -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/streaming -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/streaming-sdk -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/streaming-service -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/tool -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/spark-project/ -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/spark-project/engine-build-sdk -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/spark-project/engine-spark -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/spark-project/kylin-soft-affinity-cache -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/spark-project/source-jdbc -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/spark-project/sparder -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/spark-project/spark-common -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
mvn clean test --fail-at-end -pl src/spark-project/spark-it -DfailIfNoTests=false -Duser.timezone=GMT+8 >>${ci_output} 2>&1
echo "----------- Kylin Test Completed -----------"


echo "<Running test on following module>"
cat ${ci_output} | grep "maven-surefire-plugin:3.0.0-M5:test"

echo "<Failed test on following module>"
cat ${ci_output} | grep "Failed to execute goal org.apache.maven.plugins:maven-surefire-plugin:"

echo "<Failed cases statistics>"
cat ${ci_output} | grep "R] Tests run"
