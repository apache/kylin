---
layout: dev
title:  "How to Test"
categories: development
permalink: /development/howto_test.html
---

In general, there should be unit tests to cover individual classes; there must be integration test to cover end-to-end scenarios like build, merge, and query. Unit test must run independently (does not require an external sandbox).

## Test 2.x branches

* `mvn test` to run unit tests, which has a limited test coverage.
    * Unit tests has no external dependency and can run on any machine.
    * The unit tests do not cover end-to-end scenarios like build, merge, and query.
    * The unit tests take a few minutes to complete.
* `dev-support/test_all_against_hdp_2_2_4_2_2.sh` to run integration tests, which has the best test coverage.
    * Integration tests __better be run on a Hadoop sandbox__. We suggest to checkout a copy of code in your sandbox and direct run the test_all_against_hdp_2_2_4_2_2.sh in it. If you don't want to put codes on sandbox, refer to __More on 2.x UT/IT separation__
    * As the name indicates, the script is only for hdp 2.2.4.2, but you get the idea of how integration test run from it.
    * The integration tests start from generate random data, then build cube, merge cube, and finally query the result and compare to H2 DB.
    * The integration tests take one to two hours to complete.

## Test 1.x branches

* `mvn test` to run unit tests, which has a limited test coverage.
    * What's special about 1.x is that a hadoop/hbase mini cluster is used to cover queries in unit test.
* Run the following to run integration tests.
    * `mvn clean package -DskipTests`
    * `mvn test  -Dtest=org.apache.kylin.job.BuildCubeWithEngineTest -Dhdp.version=2.2.0.0-2041 -DfailIfNoTests=false -P sandbox`
    * `mvn test  -Dtest=org.apache.kylin.job.BuildIIWithEngineTest -Dhdp.version=2.2.0.0-2041 -DfailIfNoTests=false -P sandbox`
    * `mvn test  -fae -P sandbox`
    * `mvn test  -fae  -Dtest=org.apache.kylin.query.test.IIQueryTest -Dhdp.version=2.2.0.0-2041 -DfailIfNoTests=false -P sandbox`

## More on 2.x UT/IT separation

From Kylin 2.0 you can run UT(Unit test), environment cube provision and IT(Integration test) separately. 
Running `mvn verify -Dhdp.version=2.2.4.2-2`  (assume you're on your sandbox) is all you need to run a complete all the test suites.

It will execute the following steps sequentially:
 
    1. Build Artifacts 
    2. Run all UTs (takes few minutes) 
    3. Provision cubes on the sandbox environment for IT uasge (takes 1~2 hours) 
    4. Run all ITs (takes few tens of minutes) 
    5. verify jar stuff 

If your code change is minor and it merely requires running UT, use: 
`mvn test`
If your sandbox is already provisioned and your code change will not affect the result of sandbox provision, (and you don't want to wait hours of provision) just run the following commands to separately run UT and IT: 
`mvn test`
`mvn failsafe:integration-test`

### Cube Provision

Environment cube provision is indeed running kylin cubing jobs to prepare example cubes in the sandbox. These prepared cubes will be used by the ITs. Currently provision step is bound with the maven pre-integration-test phase, and it contains running BuildCubeWithEngine (HBase required), BuildCubeWithStream(Kafka required) and BuildIIWithStream(Kafka Required). You can run the mvn commands on you sandbox or your develop computer. For the latter case you need to set kylin.job.run.as.remote.cmd=true in __$KYLIN_HOME/examples/test_case_data/sandbox/kylin.properties__. 
Try appending `-DfastBuildMode=true` to mvn verify command to speed up provision by skipping incremental cubing. 

## More on 1.x Mini Cluster

Kylin 1.x used to move as many as possible unit test cases from sandbox to HBase mini cluster (not any more in 2.x), so that user can run tests easily in local without a hadoop sandbox. Two maven profiles are created in the root pom.xml, "default" and "sandbox". The default profile will startup a HBase Mini Cluster to prepare the test data and run the unit tests (the test cases that are not supported by Mini cluster have been added in the "exclude" list). If you want to keep using Sandbox to run test, just run `mvn test -P sandbox`

### When use the "default" profile, Kylin will

* Startup a HBase minicluster and update KylinConfig with the dynamic HBase configurations
* Create Kylin metadata tables and import six example cube tables
* Import the hbase data from a tar ball from local: `examples/test_case_data/minicluster/hbase-export.tar.gz` (the hbase-export.tar.gz will be updated on complete of running BuildCubeWithEngineTest）
* After all test cases be completed, shutdown minicluster and cleanup KylinConfig cache

### To ensure Mini cluster can run successfully, you need

* Make sure JAVA_HOME is properly set
