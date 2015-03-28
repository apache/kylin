#### Setup Dev Env
* Subscribe our developers' mail list via <dev-subscribe@kylin.incubator.apache.org>
* Fork from [GitHub](https://github.com/apache/incubator-kylin) *staging* branch, it is the bug fix branch of master.
* ...

#### Making Changes
* Discuss feature/enhancement/bug you plan to do in dev mail list, get consensus before you start.
* Create a [JIRA](https://issues.apache.org/jira/browse/KYLIN) to describe your task
* Make changes in your fork.
* Write unit test to cover your change.


#### Contribute The Work
* Pull request to *staging* branch.
* Committer review in terms of correctness, performance, design, coding style, test coverage.
* Discuss and revise if necessary.
* Committer merge code into *staging*.
* After integration test, change merges into master.


#### Wish List
Some work items for new joiners.
* Query Engine
  * Cache Calcite generated class, reduce delay into ms level and avoid full GC triggered by perm generation
  * [KYLIN-491](https://issues.apache.org/jira/browse/KYLIN-491) Derive meaningful cost in OLAP relational operator
* Job Engine
  * [KYLIN-489](https://issues.apache.org/jira/browse/KYLIN-489) Tune HDFS & HBase parameters. Requires a hadoop cluster of 10+ nodes.
  * Fork some build step (build dictionary, create hbase table) as child process, better resource cleanup and easier troubleshooting.
  * Use DoubleDouble instead of BigDecimal during cube build. Expect better CPU performance.
* ODBC/JDBC
  * Test Kylin remote JDBC with java report tools
  * [KYLIN-602](https://issues.apache.org/jira/browse/KYLIN-602) ODBC driver support excel
  * Implement ODBC async mode, streaming from Kylin and feeding to client
* Benchmark
  * [KYLIN-487](https://issues.apache.org/jira/browse/KYLIN-487) Benchmark on standard dataset


