## Run test case with HBase Mini Cluster

Kylin is moving as many as possible unit test cases from sandbox to HBase mini cluster, so that user can run tests easily in local without a hadoop sandbox; Two maven profiles are created in the root pom.xml, "default" and "sandbox"; The default profile will startup a HBase Mini Cluster to prepare the test data and run the unit tests (the test cases that are not supported by Mini cluster have been added in the "exclude" list); If you want to keep using Sandbox to run test, just run:
mvn test -P sandbox


### When use the "default" profile, Kylin will:

* 	Startup a HBase minicluster and update KylinConfig with the dynamic HBase configurations;
* 	Create Kylin metadata tables and import six example cube tables;
* 	Import the hbase data from a tar ball from local: examples/test_case_data/minicluster/hbase-export.tar.gz (the hbase-export.tar.gz will be updated on complete of running BuildCubeWithEngineTest）
* 	After all test cases be completed, shutdown minicluster and cleanup KylinConfig cache;

### To ensure Mini cluster can run successfully, you need:
* 	Make sure JAVA_HOME is properly set; 
