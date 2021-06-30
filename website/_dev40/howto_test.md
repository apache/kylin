---
layout: dev40
title:  "How to Test"
categories: development
permalink: /development40/howto_test.html
---

In general, there should be unit tests to cover individual classes; there must be integration test to cover end-to-end scenarios like build, merge, and query. Unit test must run independently (does not require an external sandbox).

## Test v4.x

* `mvn clean test` runs unit tests, which has a limited test coverage.
    * Unit tests has no external dependency and can run on any machine.
    * The unit tests do not cover end-to-end scenarios like build, merge, and query.
    * The unit tests take a few minutes to complete.
* `mvn clean test -DskipRunIt=false` runs integration tests, which has the best test coverage.
    * The integration tests start from generate random data, then build cube, merge cube, and finally query the result and compare to Spark.
    * The integration tests take about one hour to complete.

If your code changes is minor and it merely requires running UT, use:  
`mvn test`
If your code changes involve more code, you need to run UT and IT, use:
`mvn clean test -DskipRunIt=false`
