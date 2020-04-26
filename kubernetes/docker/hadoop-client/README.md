## Background
### What is hadoop-client docker image and why we need this? 
As we all know, the node you want to deploy Kylin, should has provided Hadoop 
dependency(jars and configuration files), these dependency let you have access
 to Hadoop Service, such as HDFS, HBase, Hive, which are needed by Apache Kylin. 
Unfortunately, each Hadoop distribution(CHD or HDP etc.) has its own specific jars. So, we 
can provided specific image for specific Hadoop distribution, which will make image management task
more easier. This will have following two benefits:
1. Someone who has better knowledge on Hadoop can do this work, and let kylin 
 user build their Kylin image base on provided Hadoop-Client image.
2. Upgrade Kylin will be much easier.

### Build step for CDH5.7
1. Working directory is `docker/hadoop-client/CDH57`.
2. Place Spark binary(spark-2.3.2-bin-hadoop2.7.tgz) into dir `provided-binary` directory.
3. Run `build-image.sh` to build image.

### Build step for vanilla Hadoop
1. Working directory is `docker/hadoop-client/apache-hadoop2.7`.
2. Download required hadoop binary files and put them into `hadoop-binary` directory.
3. Run `build-image.sh` to build image.

> If you are using other hadoop distribution, please considering refer to provided `Dockerfile` and write your own.

