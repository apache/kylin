# How to Run/Debug Kylin in IDE(with some backgrounds)
> This guide is an enhanced version for [How to debug Kylin in IDEA with Hadoop](https://kylin.apache.org/5.0/docs/development/how_to_debug_kylin_in_ide) , <br>
> it is not only step-by-step guide, but give more background knowledge and explanation. A video is [here](https://www.bilibili.com/video/BV19C4y1Z7AN).

## Part One - Before your read

### STEP 1: Who need this guide

If you are any developer who do following things, then this guide is for you.

- want to **deep dive into technical detail** of Kylin 5 and learn how it works
- want to fix bugs or develop new features of Kylin 5 on you own

To be specific, this guide will help you to:

- set up your development software/env of Kylin 5
- understand the source code of Kylin 5
- learn how to reproduce some bugs and try to find its root cause
- learn how to verify if your patch can fix bugs before you submits your PR

Read [background part](https://kylin.apache.org/5.0/docs/development/how_to_debug_kylin_in_ide#background).

### Step 2: Why you need Kylin 5

to be updated

### Step 3: Understand Project structure

| Folder Name | Comment                                                                                              |
|:------------|------------------------------------------------------------------------------------------------------|
| build       | Scripts for building, packaging, running Kylin                                                       |
| dev-support | Scripts and guides for contributors to develop/debug/test, for committers to release/publish website |
| kystudio    | Frontend source code, mainly using Vue.js                                                            |
| src         | Backend source code, wrote by Java & Scala, using Spark/Calcite/Hadoop/Spring etc                    |
| pom.xml     | Project definition by Apache Maven                                                                   |
| README.md   | General guide to the development of Kylin 5 project                                                  |
| LICENSE     | A must-to-have file by ASF                                                                           |
| NOTICE      | A must-to-have file by ASF                                                                           |

### Step 4: Understand Maven modules


| Module Name                | Brief Description                                                                  |              Tags               | 
|:---------------------------|:-----------------------------------------------------------------------------------|:-------------------------------:|
| Core Common                | Utility method, config entry                                                       |              Core               |
| Core Metadata              | Definition of metadata, CRUD of metadata                                           |              Core               |
| Core Metrics               | Metrics, monitor                                                                   |              Core               |
| Core Job                   | Job Engine. Define of executable, submit different job                             |              Core               |
| Core Storage               |                                                                                    |              Core               |
| Query Common               | Query parser, transformer, process                                                 |              Core               |
| Local Data Cache           | Improve query performance by caching parquet files in spark executor's disk/memory |             Add-on              |
| Spark Common               | Logic, profiler, optimizer of Spark Execution                                      |
| Query Engine Spark         |                                                                                    |
| Hive Source                | Outdated code                                                                      |
| Build Engine SDK           |                                                                                    |              Core               |
| Distributed Lock Extension | Different implementations of distributed lock                                      |             Add-on              |
| Build Engine Spark         |                                                                                    |
| Query                      | Transfer sql text to logical/physical plan and optimize using Apache Calcite       |              Core               |
| Streaming SDK              | Not ready. Used to parse Kafka message in custom way                               |    Add-on, Not-Ready-Module     |
| Streaming                  | Not ready. Make Apache Kafka as a data source for Kylin 5                          |        Not-Ready-Module         |
| Tool                       | Different tools for metadata backup, Diagnose etc                                  |              Tool               |
| Common Service             |                                                                                    |
| Datasource Service         |                                                                                    |
| Modeling Service           |                                                                                    |
| Data Loading Service       |                                                                                    |
| Query Service              | Controller&Service for SQL Query                                                   |
| Common Server              |                                                                                    |
| Job Service                |                                                                                    |
| Streaming Service          | Not ready.                                                                         |        Not-Ready-Module         |
| Data Loading Server        |                                                                                    |
| Query Server               |                                                                                    |
| Metadata Server            | Controller for CRUD of metadata                                                    |
| REST Server                | Starter of Kylin process, including Spring config files                            |             Spring              |
| Datasource SDK             | Not ready. Framework to add data source for Kylin 5                                |    Add-on, Not-Ready-Module     |
| JDBC Source                | Not ready. Make some RDBMS as a data source fro Kylin 5                            |        Not-Ready-Module         |
| Integration Test           | Major code for Integration Test                                                    |             Testing             |
| Integration Test Spark     | Some code for Integration Test                                                     |             Testing             |
| Source Assembly            | Use shade plugin to create jars for build engine in spark-submit cmd               |              Build              |
| Integration Test Server    | Some code for Integration Test                                                     |             Testing             |
| Data loading Booter        | Not ready. Starter for micro-service. Process build/refresh index/segment request  | Micro-service, Not-Ready-Module |
| Query Booter               | Not ready. Starter for micro-service. Process query request                        | Micro-service, Not-Ready-Module |
| Common Booter              | Not ready. Starter for micro-service. Process crud of metadata request             | Micro-service, Not-Ready-Module |
| JDBC Driver                | Connect Kylin using JDBC, for SQL Client or BI                                     |              Tool               |

### STEP 5: Install required software in laptop(Mac)

| Component                | Version                                    | Comment/Link                    |
|--------------------------|--------------------------------------------|---------------------------------|
| JDK                      | JDK8                                       | n/a                             |
| Apache Maven             | 3.5+                                       | n/a                             |
| IntelliJ IDEA            | IntelliJ IDEA 2023.2.2 (Community Edition) | n/a                             |
| Docker Desktop (for Mac) | 4.22.1 (118664)                            | n/a                             |
| NodeJs                   | latest                                     | https://nodejs.org/en/download/ |
| nvm                      | latest                                     | https://github.com/nvm-sh/nvm   |

After install nvm, please use `nvm install 12.14.0` to install correct version of Node.js for `kystudio`.

I checked software version in my laptop(macbook pro, 15-inch, 2018) using following command. You can try it.

```shell
(base) xiaoxiang.yu@XXYU-MBP ~ % uname -a
Darwin XXYU-MBP.local 22.6.0 Darwin Kernel Version 22.6.0: Wed Jul  5 22:21:56 PDT 2023; root:xnu-8796.141.3~6/RELEASE_X86_64 x86_64
(base) xiaoxiang.yu@XXYU-MBP ~ % java -version
java version "1.8.0_301"
Java(TM) SE Runtime Environment (build 1.8.0_301-b09)
Java HotSpot(TM) 64-Bit Server VM (build 25.301-b09, mixed mode)
(base) xiaoxiang.yu@XXYU-MBP ~ % mvn -version
Apache Maven 3.8.2 (ea98e05a04480131370aa0c110b8c54cf726c06f)
Maven home: /Users/xiaoxiang.yu/LacusDir/lib/apache-maven-3.8.2
Java version: 1.8.0_301, vendor: Oracle Corporation, runtime: /Library/Java/JavaVirtualMachines/jdk1.8.0_301.jdk/Contents/Home/jre
Default locale: en_CN, platform encoding: UTF-8
OS name: "mac os x", version: "10.16", arch: "x86_64", family: "mac"
(base) xiaoxiang.yu@XXYU-MBP ~ % nvm -v
0.39.1
(base) xiaoxiang.yu@XXYU-MBP ~ % nvm list
->     v12.14.0
       v16.16.0
         system
default -> 12.14.0 (-> v12.14.0)
(base) xiaoxiang.yu@XXYU-MBP ~ % node -v
v12.14.0
(base) xiaoxiang.yu@XXYU-MBP ~ % docker -v
Docker version 24.0.5, build ced0996
(base) xiaoxiang.yu@XXYU-MBP ~ % date
Wed Sep 20 11:15:45 CST 2023
```

Make sure these port is available to use.

### STEP 6: Prepare a linux machine(Optional)

Use real Hadoop Cluster(compare to local mode) make you test your patch in immersive way and reproduce issue easily.

When you run Kylin 5 in laptop, and submitting spark job to building cube/mode at the same time. 
These programs may consume a lot of hardware resource, and you will suffer poor experience. So it is better not to run hadoop on your laptop.

If you like, you can choose to connect to your own test/production Hadoop cluster.

```shell
kylin@worker-03:~$ docker -v
Docker version 20.10.17, build 100c701
kylin@worker-03:~$ uname -a
Linux worker-03 5.4.0-135-generic #152-Ubuntu SMP Wed Nov 23 20:19:22 UTC 2022 x86_64 x86_64 x86_64 GNU/Linux
kylin@worker-03:~$ free -h
total        used        free      shared  buff/cache   available
Mem:           47Gi       2.8Gi        37Gi       2.0Mi       8.0Gi        44Gi
Swap:         8.0Gi          0B       8.0Gi
```


### Attention
This guide is verified using this [verified tag](https://github.com/apache/kylin/releases/tag/ide-run-2023) <br>
in 2023-09-22 by xxyu, requiring macOS and recommended software.

Since future commits may change behavior or have a chance to break something, <br>
you are suggested to go through this guide using commit which I verified.

----
## Part Two - Set up Development


### STEP 1: Fetch source code

```shell
git clone https://github.com/apache/kylin.git --single-branch --branch ide-run-2023 demo-kylin5-local-run
export PROJECT_DIR=~/demo-kylin5-local-run
```

> **Attention**: <br>
> And root path of source code is replaced with $PROJECT_DIR at following doc. <br>
> All following commands are executed in $PROJECT_DIR .

### STEP 2: Build source using maven

```shell
mvn clean install -DskipTests
```

Why you need run `mvn install/package` ?
1. Some jars are built by `maven-shade-plugin` in `$PROJECT_DIR/src/assembly`, and it is required in later step(`$PROJECT_DIR/src/assembly/target/kylin-assembly-5.0.0-SNAPSHOT-job.jar`)
2. Some source to be generated by javacc-plugin.

This process may take 10-20 minutes.

```shell
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for Apache Kylin 5 5.0.0-SNAPSHOT:
[INFO]
[INFO] Apache Kylin 5 ..................................... SUCCESS [ 19.699 s]
[INFO] Kylin - Core Common ................................ SUCCESS [02:10 min]
[INFO] Kylin - Core Metadata .............................. SUCCESS [01:45 min]
[INFO] Kylin - Core Metrics ............................... SUCCESS [ 27.640 s]
[INFO] Kylin - Core Job ................................... SUCCESS [ 27.562 s]
[INFO] Kylin - Core Storage ............................... SUCCESS [  5.791 s]
[INFO] Kylin - Query Common ............................... SUCCESS [ 17.910 s]
[INFO] Kylin - Local Data Cache ........................... SUCCESS [ 33.482 s]
[INFO] Kylin - Spark Common ............................... SUCCESS [01:14 min]
[INFO] Kylin - Query Engine Spark ......................... SUCCESS [02:04 min]
[INFO] Kylin - Hive Source ................................ SUCCESS [ 11.103 s]
[INFO] Kylin - Build Engine ............................... SUCCESS [  3.453 s]
[INFO] Kylin - Distributed Lock Extension ................. SUCCESS [  6.703 s]
[INFO] Kylin - Build Engine Spark ......................... SUCCESS [01:45 min]
[INFO] Kylin - Query ...................................... SUCCESS [ 34.497 s]
[INFO] Kylin - Streaming SDK .............................. SUCCESS [  3.539 s]
[INFO] Kylin - Streaming .................................. SUCCESS [ 57.014 s]
[INFO] Kylin - Tool ....................................... SUCCESS [ 28.052 s]
[INFO] Kylin - Common Service ............................. SUCCESS [ 26.044 s]
[INFO] Kylin - Datasource Service ......................... SUCCESS [ 26.970 s]
[INFO] Kylin - Modeling Service ........................... SUCCESS [ 29.045 s]
[INFO] Kylin - Data Loading Service ....................... SUCCESS [ 21.783 s]
[INFO] Kylin - Query Service .............................. SUCCESS [ 24.721 s]
[INFO] Kylin - Common Server .............................. SUCCESS [ 11.979 s]
[INFO] Kylin - Job Service ................................ SUCCESS [ 11.572 s]
[INFO] Kylin - Streaming Service .......................... SUCCESS [  8.576 s]
[INFO] Kylin - Data Loading Server ........................ SUCCESS [  9.981 s]
[INFO] Kylin - Query Server ............................... SUCCESS [ 22.769 s]
[INFO] Kylin - Metadata Server ............................ SUCCESS [ 12.105 s]
[INFO] Kylin - REST Server ................................ SUCCESS [ 16.878 s]
[INFO] Kylin - Datasource SDK ............................. SUCCESS [  6.534 s]
[INFO] Kylin - JDBC Source ................................ SUCCESS [ 16.812 s]
[INFO] Kylin - Integration Test ........................... SUCCESS [ 26.048 s]
[INFO] Kylin - Integration Test Spark ..................... SUCCESS [ 17.564 s]
[INFO] Kylin - Source Assembly ............................ SUCCESS [ 51.755 s]
[INFO] Kylin - Integration Test Server .................... SUCCESS [ 15.409 s]
[INFO] Kylin - Data loading Booter ........................ SUCCESS [  6.654 s]
[INFO] Kylin - Query Booter ............................... SUCCESS [  5.126 s]
[INFO] Kylin - Common Booter .............................. SUCCESS [  4.365 s]
[INFO] Kylin - JDBC Driver ................................ SUCCESS [  7.785 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  20:02 min
[INFO] Finished at: 2023-09-20T11:25:25+08:00
[INFO] ------------------------------------------------------------------------
```

### STEP 3: Build source of front-end

```shell
cd kystudio
npm install
```

### Step 4: Download Spark

```shell
bash build/release/download-spark.sh
```

By default, the `SPARK_HOME` is pointed to `$PROJECT_DIR/build/spark`. You are free to move spark directory to other place.

### STEP 5: Import and configure project in IDE 

#### 1. Install required IDEA plugins
- Scala Plugin
- Lombok Plugin
- JavaCC Plugin

#### 2. Set up Java SDK and Scala SDK for project

- JDK 1.8
- Scala 2.12

#### 3. Load Maven modules and build project

1. Load Maven modules
   1. Find Maven tool window <br>
   Find the tools bar of IDE, click following text in order: <br>
   "View" -> "Tool Windows" -> "Maven"
   2. Click "Reload All Maven Projects"
   3. Wait all process finished
2. Build project in IDE
   1. Find the tools bar of IDE, click following text in order:<br>
   "Build" -> "Build Project"
   2. Wait all process finished
3. Check build status by running any ut (for example `HashFunctionTest`).

### STEP 7: Start Hadoop Containers

1. If you decided to running Hadoop processes in docker containers, you can export `DOCKER_HOST` to correct host/ip.
   ```shell
   export DOCKER_HOST=ssh://kylin@worker-03
   ```
2. Start Hadoop processes in Docker by:
   ```shell
   docker compose -f "${PROJECT_DIR}/dev-support/contributor/sandbox/docker-compose.yml" up
   ```
3. Check status of Hadoop processes in Docker by:
   ```shell
   docker compose -f "${PROJECT_DIR}/dev-support/contributor/sandbox/docker-compose.yml" ps
   ```
4. Init metadata(MySQL) for Kylin
   Use `docker exec -it mysql bash` 
   ```shell
   mysql -uroot -proot
   CREATE DATABASE IF NOT EXISTS kylin;
   ```
5. Init working dir for Kylin in HDFS
   Use `docker exec -it namenode bash`
   ```shell
   hadoop dfs -mkdir -p '/kylin/spark-history'
   ```
6. Prepare sample data
   ```shell
   docker cp ${PROJECT_DIR}/src/examples/sample_cube/data datanode:/tmp/ssb
   docker cp ${PROJECT_DIR}/src/examples/sample_cube/create_sample_ssb_tables.sql hiveserver:/tmp/
  
   docker exec datanode bash -c "hdfs dfs -mkdir -p /tmp/sample_cube/data \
            && hdfs dfs -put /tmp/ssb/* /tmp/sample_cube/data/"
   docker exec hiveserver bash -c "hive -e 'CREATE DATABASE IF NOT EXISTS SSB' \
            && hive --hivevar hdfs_tmp_dir=/tmp --database SSB -f /tmp/create_sample_ssb_tables.sql"
   ```

### STEP 8: Running Configuration 

IDEA official reference is https://www.jetbrains.com/help/idea/run-debug-configuration.html https://www.jetbrains.com/help/idea/run-debug-configuration-java-application.html#more_options.

#### ENVIRONMENT VAR

```shell 
KYLIN_HOME=$PROJECT_DIR
KYLIN_CONF=$PROJECT_DIR/dev-support/contributor/sandbox/conf
SPARK_HOME=
HADOOP_CONF_DIR=$PROJECT_DIR/dev-support/contributor/sandbox/conf
HADOOP_USER_NAME=root
```

#### SYSTEM PROPERTY
- Spring Framework
spring.profiles.active=sandbox,docker

#### MAIN CLASS 
org.apache.kylin.rest.BootstrapServer

#### CLASSPATH
Module **kylin-server**, and add option **INCLUDE_PROVIDED_SCOPE**

#### WORKING DIR
Module **kylin-server**, or %MODULE_WORKING_DIR% .

### STEP 8.1: More Misc Configuration

#### Log4j2 Configuration
Modify $PROJECT_DIR/src/server/src/resources/log4j2.xml to change log level etc.
Add `-Dlog4j2.debug` in vm options if you need debug.

#### Kylin Configuration
$PROJECT_DIR/dev-support/contributor/sandbox/conf/kylin.properties

#### Calcite Debug

#### Spring Configuration

### Important Breakpoints

| Class Name                       | Desc |
|----------------------------------|------|
| KylinPrepareEnvListener          |      |
| AppInitializer                   |      |
| BootstrapServer                  |      |
| KylinPropertySourceConfiguration |      |
----

## Troubles shooting & Tips

#### How to clean up

- Remove docker containers
```shell
docker compose -f "${PROJECT_DIR}/dev-support/contributor/sandbox/docker-compose.yml" down
```

- Uninstall IDEA https://www.jetbrains.com/help/idea/uninstall.html#standalone
```shell
rm -rf /Applications/IntelliJ\ IDEA\ CE.app
rm -rf ~/Library/Application\ Support/JetBrains/IdeaIC*
rm -rf ~/Library/Caches/JetBrains/IdeaIC*
```

- Clean local installed snapshot artificial
```shell
rm -rf  ~/.m2/repository/org/apache/kylin
```

- Clean source code
```shell
rm -rf $PROJECT_DIR
```

#### Fix Invalid method name: 'get_all_functions'
```sh
org.apache.hadoop.hive.ql.metadata.HiveException: org.apache.thrift.TApplicationException: Invalid method name: 'get_all_functions'
	at org.apache.hadoop.hive.ql.metadata.Hive.getAllFunctions(Hive.java:3904) ~[hive-exec-2.3.9.jar:2.3.9]
```

Maybe spark.sql.hive.metastore.jars is pointed to correct path in `$PROJECT_DIR/dev-support/contributor/sandbox/conf/kylin.properties`.

#### Performance Improvement

When you submit multiple jobs at the same times, you pc might suffer from bad performance. 
- Reduce the max concurrent number of job. 

#### Cannot download logs of build engine

```shell
ERROR [local_run] [http-nio-7070-exec-9] execution.NExecutableManager : get sample data from hdfs log file [/kylin/kylin_metadata/local_run/job_tmp/4edaf665-430a-8cc0-620e-b9f7ae2458bc-98cbd8b9-fee2-44d4-0383-b8b603348e94/01//execute_output.json.1695364459595.log] failed!
org.apache.hadoop.hdfs.CannotObtainBlockLengthException: Cannot obtain block length for LocatedBlock{BP-258157769-172.22.0.6-1695103912779:blk_1073742602_1778; getBlockSize()=26457; corrupt=false; offset=0; locs=[DatanodeInfoWithStorage[172.22.0.3:50010,DS-ca2d8134-89bd-49d1-af55-680cea736111,DISK]]} of /kylin/kylin_metadata/local_run/job_tmp/4edaf665-430a-8cc0-620e-b9f7ae2458bc-98cbd8b9-fee2-44d4-0383-b8b603348e94/01/execute_output.json.1695364459595.log
	at org.apache.hadoop.hdfs.DFSInputStream.readBlockLength(DFSInputStream.java:470) ~[hadoop-hdfs-client-2.10.1.jar:?]
```


#### Query Failed by InvalidClassException

```shell
Caused by: java.io.InvalidClassException: org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat; 
local class incompatible: stream classdesc serialVersionUID = 8961733539262042287, local class serialVersionUID = -27198871445502271
```

You are using different version of Spark, unify them(1. SPARK_HOME 2. `spark.version` in $PROJECT_DIR/pom.xml).