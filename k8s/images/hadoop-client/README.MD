Build Step
1. Please download/copy your hadoop client to hadoop-client folder, which includes hadoop, hive, hbase, spark and zookeeper.
2. Please copy your hadoop config to config folder, which includes hadoop, hive, hbase and krb5.
2. Update related version variables in Dockerfile.
3. Run docker build -t "apachekylin/kylin-hdp-client:${VERSION}"