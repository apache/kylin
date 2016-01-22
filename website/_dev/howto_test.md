---
layout: dev
title:  "How to Test"
categories: development
permalink: /development/howto_test.html
---

In general, there should be unit tests to cover individual classes; there must be integration test to cover end-to-end scenarios like build, merge, and query. Unit test must run independently (does not require an external sandbox).


## 2.x branches

* `mvn test` to run unit tests, which has a limited test coverage.
    * Unit tests has no external dependency and can run on any machine.
    * The unit tests do not cover end-to-end scenarios like build, merge, and query.
    * The unit tests take a few minutes to complete.
* `dev-support/test_all_against_hdp_2_2_4_2_2.sh` to run integration tests, which has the best test coverage.
    * Integration tests __must run on a Hadoop sandbox__. Make sure all changes you want to test are avaiable on sandbox.
    * As the name indicates, the script is only for hdp 2.2.4.2, but you get the idea of how integration test run from it.
    * The integration tests start from generate random data, then build cube, merge cube, and finally query the result and compare to H2 DB.
    * The integration tests take a few hours to complete.


## 1.x branches

* `mvn test` to run unit tests, which has a limited test coverage.
    * What's special about 1.x is that a hadoop/hbase mini cluster is used to cover queries in unit test.
* Run the following to run integration tests.
    * `mvn clean package -DskipTests`
    * `mvn test  -Dtest=org.apache.kylin.job.BuildCubeWithEngineTest -Dhdp.version=2.2.0.0-2041 -DfailIfNoTests=false -P sandbox`
    * `mvn test  -Dtest=org.apache.kylin.job.BuildIIWithEngineTest -Dhdp.version=2.2.0.0-2041 -DfailIfNoTests=false -P sandbox`
    * `mvn test  -fae -P sandbox`
    * `mvn test  -fae  -Dtest=org.apache.kylin.query.test.IIQueryTest -Dhdp.version=2.2.0.0-2041 -DfailIfNoTests=false -P sandbox`


## More on 1.x Mini Cluster

Kylin 1.x used to move as many as possible unit test cases from sandbox to HBase mini cluster (not any more in 2.x), so that user can run tests easily in local without a hadoop sandbox. Two maven profiles are created in the root pom.xml, "default" and "sandbox". The default profile will startup a HBase Mini Cluster to prepare the test data and run the unit tests (the test cases that are not supported by Mini cluster have been added in the "exclude" list). If you want to keep using Sandbox to run test, just run `mvn test -P sandbox`


### When use the "default" profile, Kylin will

* Startup a HBase minicluster and update KylinConfig with the dynamic HBase configurations
* Create Kylin metadata tables and import six example cube tables
* Import the hbase data from a tar ball from local: `examples/test_case_data/minicluster/hbase-export.tar.gz` (the hbase-export.tar.gz will be updated on complete of running BuildCubeWithEngineTest）
* After all test cases be completed, shutdown minicluster and cleanup KylinConfig cache

### To ensure Mini cluster can run successfully, you need
* Make sure JAVA_HOME is properly set
