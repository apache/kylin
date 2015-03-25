#### Setup Dev Env
* subscribe our developers' mail list via <dev-subscribe@kylin.incubator.apache.org>
* Fork on [GitHub](https://github.com/KylinOLAP)
* ...

#### Making Changes
* Raise an issue on GitHub, describe the feature/enhancement/bug
* Discuss with others in google group or issue comments, make sure the proposed changes fit in with what others are doing and have planned for the project
* Make changes in your fork
* Write unit test if no existing cover your change
* Push to GitHub under your fork


#### Contribute The Work
* Raise a pull request on GitHub, include both **code** and **test**, link with **related issue**
* Committer will review in terms of correctness, performance, design, coding style, test coverage
* Discuss and revise if necessary
* Finally committer merge code into main branch


#### Wish List
Some potential work items
* Query Engine
  * Cache generated class, reduce delay into ms level and avoid full GC triggered by perm generation
  * [Issue #14](https://github.com/KylinOLAP/Kylin/issues/14) Derive meaningful cost in OLAP relational operator
  * [Issue #15](https://github.com/KylinOLAP/Kylin/issues/15) Implement multi-column distinct count
* Metadata
  * [Issue #7](https://github.com/KylinOLAP/Kylin/issues/7) Merge multiple hbase tables
* Job Engine
  * [Issue #16](https://github.com/KylinOLAP/Kylin/issues/16) Tune HDFS & HBase parameters
  * [Issue #17](https://github.com/KylinOLAP/Kylin/issues/17) Increase HDFS block size 1GB or close
  * Shell command to support kill operation
  * Use DoubleDouble instead of BigDecimal during cube build
  * Drop quartz dependency, assess the cost/benefit first
  * Cardinality run as one step job, allows progress tracking
* ODBC/JDBC
  * Test Kylin remote JDBC with java report tools
  * Implement ODBC async mode, fetching from Kylin and feeding to client in parallel
* Benchmark
  * [Issue #18](https://github.com/KylinOLAP/Kylin/issues/18) Benchmark on standard dataset


