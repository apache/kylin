FAQ on Kylin Installation and Usage
---
#### Some NPM error causes ERROR exit (中国大陆地区用户请特别注意此问题)?
Check out https://github.com/KylinOLAP/Kylin/issues/35

#### Can't get master address from ZooKeeper" when installing Kylin on Hortonworks Sandbox
Check out https://github.com/KylinOLAP/Kylin/issues/9.

#### Install scripted finished in my virtual machine, but cannot visit via http://localhost:9080
Check out https://github.com/KylinOLAP/Kylin/issues/12.

#### Map Reduce Job information can't display on sandbox deployment
Check out https://github.com/KylinOLAP/Kylin/issues/40

#### Install Kylin on CDH 5.2 or Hadoop 2.5.x
Check out discussion: https://groups.google.com/forum/?utm_medium=email&utm_source=footer#!msg/kylin-olap/X0GZfsX1jLc/nzs6xAhNpLkJ
```
I was able to deploy Kylin with following option in POM.
<hadoop2.version>2.5.0</hadoop2.version>
<yarn.version>2.5.0</yarn.version>
<hbase-hadoop2.version>0.98.6-hadoop2</hbase-hadoop2.version>
<zookeeper.version>3.4.5</zookeeper.version>
<hive.version>0.13.1</hive.version>
My Cluster is running on Cloudera Distribution CDH 5.2.0.
```

#### Unable to load a big cube as HTable, with java.lang.OutOfMemoryError: unable to create new native thread
HBase (as of writing) allocates one thread per region when bulk loading a HTable. Try reduce the number of regions of your cube by setting its "capacity" to "MEDIUM" or "LARGE". Also tweaks OS & JVM can allow more threads, for example see [this article](http://blog.egilh.com/2006/06/2811aspx.html).

#### Failed to run BuildCubeWithEngineTest, saying failed to connect to hbase while hbase is active
User may get this error when first time run hbase client, please check the error trace to see whether there is an error saying couldn't access a folder like "/hadoop/hbase/local/jars"; If that folder doesn't exist, create it.

#### SUM(field) returns a negtive result while all the numbers in this field are > 0
If a column is declared as integer in Hive, the SQL engine (calcite) will use column's type (integer) as the data type for "SUM(field)", while the aggregated value on this field may exceed the scope of integer; in that case the cast will cause a negtive value be returned; The workround is, alter that column's type to BIGINT in hive, and then sync the table schema to Kylin (the cube doesn't need rebuild); Keep in mind that, always declare as BIGINT in hive for an integer column which would be used as a measure in Kylin; See hive number types: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-NumericTypes

#### Why Kylin need extract the distinct columns from Fact Table before building cube?
Kylin uses dictionary to encode the values in each column, this greatly reduce the cube's storage size. To build the dictionary, Kylin need fetch the distinct values for each column.

#### Why Kylin calculate the HIVE table cardinality?
The cardinality of dimensions is an important measure of cube complexity. The higher the cardinality, the bigger the cube, and thus the longer to build and the slower to query. Cardinality > 1,000 is worth attention and > 1,000,000 should be avoided at best effort. For optimal cube performance, try reduce high cardinality by categorize values or derive features.
