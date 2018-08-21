---
layout: dev
title:  "How to Test"
categories: development
permalink: /development/howto_test.html
---

In general, there should be unit tests to cover individual classes; there must be integration test to cover end-to-end scenarios like build, merge, and query. Unit test must run independently (does not require an external sandbox).

## Test v1.5 and above

* `mvn clean test` runs unit tests, which has a limited test coverage.
    * Unit tests has no external dependency and can run on any machine.
    * The unit tests do not cover end-to-end scenarios like build, merge, and query.
    * The unit tests take a few minutes to complete.
* `dev-support/test_all_against_hdp_2_4_0_0_169.sh` runs integration tests, which has the best test coverage.
    * Integration tests __better be run on a Hadoop sandbox__. We suggest to checkout a copy of code in your sandbox and direct run the test_all_against_hdp_2_2_4_2_2.sh in it. If you don't want to put codes on sandbox, refer to __More on v1.5 UT/IT separation__
    * As the name indicates, the script is only for hdp 2.2.4.2, but you get the idea of how integration test run from it.
    * The integration tests start from generate random data, then build cube, merge cube, and finally query the result and compare to H2 DB.
    * The integration tests take one to two hours to complete.
* `nohup dev-support/test_all_against_hdp_2_4_0_0_169.sh < /dev/null 2>&1 > nohup.out &` runs IT in an unattended mode.


## More on v1.5 UT/IT separation

Running `mvn verify -Dhdp.version=2.4.0.0-169` (assume you're on your sandbox) is all you need to run a complete all the test suites.

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

Test cube provision is indeed running kylin cubing jobs to prepare example cubes in the sandbox. These prepared cubes will be used by the ITs. Currently provision step is bound with the maven pre-integration-test phase, and it contains running BuildCubeWithEngine (HBase required) and BuildCubeWithStream(Kafka required). You can run the mvn commands on you sandbox or your development computer. For the latter case you need to set `kylin.job.use-remote-cli=true`in __$KYLIN_HOME/examples/test_case_data/sandbox/kylin.properties__. 
Try appending `-DfastBuildMode=true` to mvn verify command to speed up provision by skipping incremental cubing. 

