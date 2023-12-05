# Preview latest Kylin (5.x)

## [Image Tag Information](https://hub.docker.com/r/apachekylin/apache-kylin-standalone)
| Tag                  | Image Contents                                                             | Comment & Publish Date                                                                                                                                   |
|----------------------|----------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| 5.0-beta             | [**Recommended for users**] The official 5.0.0-beta with Spark bundled.    | Uploaded at 2023-09-08, worked fine on Docker Desktop Mac 4.3.0 & 4.22.1(and Windows) ,                                                                  |
| kylin-4.0.1-mondrian | The official Kylin 4.0.1 with **MDX** function enabled                     | Uploaded at 2022-05-13                                                                                                                                   | 
| 5-dev                | [**For developer only**] Kylin 5.X package with some sample data/tools etc | Uploaded at 2023-11-21, this image for developer to debug and test Kylin 5.X source code if he/her didn't have a Hadoop env                              |
| 5.x-base-dev-only    | [**For maintainer only**] Hadoop, Hive, Zookeeper, MySQL, JDK8             | Uploaded at 2023-09-07, this is the base image for all Kylin 5.X image, so it didn't contain Kylin package, see file `Dockerfile_hadoop` for information |

## Why you need Kylin 5

These are the highlight features of Kylin 5, if you are interested, please visit https://kylin.apache.org/5.0/ for detail information.

#### More flexible and enhanced data model
- Allow adding new dimensions and measures to the existing data model
- The model adapts to table schema changes while retaining the existing index at the best effort
- Support last-mile data transformation using Computed Column
- Support raw query (non-aggregation query) using Table Index
- Support changing dimension table (SCD2)
#### Simplified metadata design
- Merge DataModel and CubeDesc into new DataModel
- Add DataFlow for more generic data sequence, e.g. streaming alike data flow
- New metadata AuditLog for better cache synchronization
#### More flexible index management (was cuboid)
- Add IndexPlan to support flexible index management
- Add IndexEntity to support different index type
- Add LayoutEntity to support different storage layouts of the same Index
#### Toward a native and vectorized query engine
- Experiment: Integrate with a native execution engine, leveraging Gluten
- Support async query
- Enhance cost-based index optimizer
#### More
- Build engine refactoring and performance optimization
- New WEB UI based on Vue.js, a brand new front-end framework, to replace AngularJS
- Smooth modeling process in one canvas

### Attention!
After the time of release of Kylin 5.0.0 , **Kylin 4.X and older version** will be set in **retired** status(NOT in active development).

## How to preview Kylin 5

Deploy a Kylin 5.X instance without any pre-deployed hadoop component by following command:

```shell
docker run -d \
  --name Kylin5-Machine \
  --hostname Kylin5-Machine \
  -m 8G \
  -p 7070:7070 \
  -p 8088:8088 \
  -p 9870:9870 \
  -p 8032:8032 \
  -p 8042:8042 \
  -p 2181:2181 \
  apachekylin/apache-kylin-standalone:5.0-beta

docker logs --follow Kylin5-Machine
```

When you enter these two commands, the logs will scroll
out in terminal and the process will continue for 3-5 minutes.

```

===============================================================================
*******************************************************************************
|
|   Start MySQL at Fri Sep  8 03:35:26 UTC 2023
|   Command: service mysql start
|
 * Starting MySQL database server mysqld
su: warning: cannot change directory to /nonexistent: No such file or directory
   ...done.
[Start MySQL] succeed.

===============================================================================
*******************************************************************************
|
|   Create Database at Fri Sep  8 03:35:35 UTC 2023
|   Command: mysql -uroot -p123456 -e CREATE DATABASE IF NOT EXISTS kylin default charset utf8mb4 COLLATE utf8mb4_general_ci;
|
mysql: [Warning] Using a password on the command line interface can be insecure.
[Create Database] succeed.

===============================================================================
*******************************************************************************
|
|   Init Hive at Fri Sep  8 03:35:35 UTC 2023
|   Command: schematool -initSchema -dbType mysql
|
SLF4J: Class path contains multiple SLF4J bindings.


...
...


===============================================================================
*******************************************************************************
|
|   Start Kylin Instance at Fri Sep  8 03:38:13 UTC 2023
|   Command: /home/kylin/apache-kylin-5.0.0-beta-bin/bin/kylin.sh -v start
|
Turn on verbose mode.
java is /usr/lib/jvm/java-1.8.0-openjdk-amd64/bin/java
Starting Kylin...
This user don't have permission to run crontab.

Kylin is checking installation environment, log is at /home/kylin/apache-kylin-5.0.0-beta-bin/logs/check-env.out

Checking Kerberos
...................................................[SKIP]
Checking OS Commands
...................................................[PASS]
Checking Hadoop Configuration
...................................................[PASS]
Checking Permission of HDFS Working Dir
...................................................[PASS]
Checking Java Version
...................................................[PASS]
Checking Kylin Config
...................................................[PASS]
Checking Ports Availability
...................................................[PASS]
Checking Spark Driver Host
...................................................[WARN]
WARNING:
    Current kylin_engine_deploymode is 'client'.
    WARN: 'kylin.storage.columnar.spark-conf.spark.driver.host' is missed, it may cause some problems.
    WARN: 'kylin.engine.spark-conf.spark.driver.host' is missed, it may cause some problems.
Checking Spark Dir
...................................................[PASS]
Checking Spark Queue
...................................................[SKIP]
Checking Spark Availability
...................................................[PASS]
Checking Metadata Accessibility
...................................................[PASS]
Checking Zookeeper Role
...................................................[PASS]
Checking Query History Accessibility
...................................................[PASS]

>   WARN: Command lsb_release is not accessible. Please run on Linux OS.
>   WARN: Command 'lsb_release -a' does not work. Please run on Linux OS.
>   WARN: 'dfs.client.read.shortcircuit' is not enabled which could impact query performance. Check /home/kylin/apache-kylin-5.0.0-beta-bin/hadoop_conf/hdfs-site.xml
>   Available YARN RM cores: 8
>   Available YARN RM memory: 8192M
>   The max executor instances can be 8
>   The current executor instances is 1
Checking environment finished successfully. To check again, run 'bin/check-env.sh' manually.

KYLIN_HOME is:/home/kylin/apache-kylin-5.0.0-beta-bin
KYLIN_CONFIG_FILE is:/home/kylin/apache-kylin-5.0.0-beta-bin/conf/kylin.properties
SPARK_HOME is:/home/kylin/apache-kylin-5.0.0-beta-bin/spark
Retrieving hadoop config dir...
KYLIN_JVM_SETTINGS is -server -Xms1g -Xmx8g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=16m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark  -Xloggc:/home/kylin/apache-kylin-5.0.0-beta-bin/logs/kylin.gc.%p  -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=64M -XX:-OmitStackTraceInFastThrow -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector -DAsyncLogger.RingBufferSize=8192
KYLIN_DEBUG_SETTINGS is not set, will not enable remote debuging
KYLIN_LD_LIBRARY_SETTINGS is not set, it is okay unless you want to specify your own native path
SPARK_HDP_VERSION is set to 'hadoop'
Export SPARK_HOME to /home/kylin/apache-kylin-5.0.0-beta-bin/spark
Checking Zookeeper role...
Checking Spark directory...
KYLIN_HOME is:/home/kylin/apache-kylin-5.0.0-beta-bin
KYLIN_CONFIG_FILE is:/home/kylin/apache-kylin-5.0.0-beta-bin/conf/kylin.properties
SPARK_HOME is:/home/kylin/apache-kylin-5.0.0-beta-bin/spark
KYLIN_JVM_SETTINGS is -server -Xms1g -Xmx8g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=16m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark  -Xloggc:/home/kylin/apache-kylin-5.0.0-beta-bin/logs/kylin.gc.%p  -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=64M -XX:-OmitStackTraceInFastThrow -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector -DAsyncLogger.RingBufferSize=8192
KYLIN_DEBUG_SETTINGS is not set, will not enable remote debuging
KYLIN_LD_LIBRARY_SETTINGS is not set, it is okay unless you want to specify your own native path
SPARK_HDP_VERSION is set to 'hadoop'
Export SPARK_HOME to /home/kylin/apache-kylin-5.0.0-beta-bin/spark
KYLIN_HOME is:/home/kylin/apache-kylin-5.0.0-beta-bin
KYLIN_CONFIG_FILE is:/home/kylin/apache-kylin-5.0.0-beta-bin/conf/kylin.properties
SPARK_HOME is:/home/kylin/apache-kylin-5.0.0-beta-bin/spark
KYLIN_JVM_SETTINGS is -server -Xms1g -Xmx8g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=16m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark  -Xloggc:/home/kylin/apache-kylin-5.0.0-beta-bin/logs/kylin.gc.%p  -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=64M -XX:-OmitStackTraceInFastThrow -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector -DAsyncLogger.RingBufferSize=8192
KYLIN_DEBUG_SETTINGS is not set, will not enable remote debuging
KYLIN_LD_LIBRARY_SETTINGS is not set, it is okay unless you want to specify your own native path
SPARK_HDP_VERSION is set to 'hadoop'
Export SPARK_HOME to /home/kylin/apache-kylin-5.0.0-beta-bin/spark
Kylin is starting. It may take a while. For status, please visit http://Kylin5-Machine:7070/kylin/index.html.
You may also check status via: PID:9781, or Log: /home/kylin/apache-kylin-5.0.0-beta-bin/logs/kylin.log.
[Start Kylin Instance] succeed.
Checking Check Env Script's status...
/home/kylin/apache-kylin-5.0.0-beta-bin/bin/check-env-bypass
+
Check Check Env Script succeed.
0
Kylin service is already available for you to preview.
```

Finally, the following message indicates that the Kylin is ready :

```
Kylin service is already available for you to preview.
```

After that, please press `Ctrl + C` to exit `docker logs`, and visit Kylin web UI.


| Service Name | URL                         |
|--------------|-----------------------------|
| Kylin        | http://localhost:7070/kylin |
| Yarn         | http://localhost:8088       |
| HDFS         | http://localhost:9870       |

When you log in Kylin web UI, please remember your username is **ADMIN** , and password is **KYLIN** .


To stop and remove the container, use these commands.
```
docker stop Kylin5-Machine
docker rm Kylin5-Machine
```


### Notes

If you are using mac docker desktop, please ensure that you have set Resources: Memory=8GB and Cores=6 cores at least,
so that can run kylin standalone on docker well.

If you are interested in `Dockerfile`, please visit https://github.com/apache/kylin/blob/kylin5/dev-support/release-manager/standalone-docker/all_in_one/Dockerfile_kylin .

If you want to configure and restart Kylin instance,
you can use `docker exec -it Kylin5-Machine bash` to login the container.
Kylin is deployed at `/home/kylin/apache-kylin-{VERSION}-bin`.

If you find some issues, please send email to Kylin's user mailing list.

For how to connect to PowerBI, here is  [a discussion in mailing list](https://lists.apache.org/thread/74pxjcx58t3m83r6o9b1hrzjjd40lhy4) .

---------
# Preview Inactive version (4.x & 3.x)

After the time of release of Kylin 5.0.0 , **Kylin 4.X and older version** will be set in **retired** status(NOT in active development),
following is the command for Kylin 4.X:

```
docker run -d \
-m 8G \
-p 7070:7070 \
-p 8088:8088 \
-p 50070:50070 \
-p 8032:8032 \
-p 8042:8042 \
-p 2181:2181 \
apachekylin/apache-kylin-standalone:4.0.0
```

and the command for Kylin 4.X with mondrian:

```
docker run -d \
-m 8G \
-p 7070:7070 \
-p 7080:7080 \
-p 8088:8088 \
-p 50070:50070 \
-p 8032:8032 \
-p 8042:8042 \
-p 2181:2181 \
apachekylin/apache-kylin-standalone:kylin-4.0.1-mondrian
```

and the command for Kylin 3.X:

```sh
docker run -d \
-m 8G \
-p 7070:7070 \
-p 8088:8088 \
-p 50070:50070 \
-p 8032:8032 \
-p 8042:8042 \
-p 16010:16010 \
apachekylin/apache-kylin-standalone:3.1.0
```

----- 

## Note

If you are using mac docker desktop, please ensure that you have set Resources: Memory=8GB and Cores=6 cores at least so that can run kylin standalone on docker well.

如果是使用 mac docker desktop 的用户，请将 docker desktop 中 Resource 的内存至少设置为 8gb 以及 6 core，以保证能流畅运行 kylin standalone on docker.

-----

For user, please visit http://kylin.apache.org/docs/install/kylin_docker.html for detail.

对于中国用户，请参阅 http://kylin.apache.org/cn/docs/install/kylin_docker.html

------

JIRA : https://issues.apache.org/jira/browse/KYLIN-4114.

For any suggestion, please contact us via Kylin's mailing list: user@kylin.apache.org.
