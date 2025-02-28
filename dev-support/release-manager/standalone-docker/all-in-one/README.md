# Preview latest Kylin (5.x)

## [Image Tag Information](https://hub.docker.com/r/apachekylin/apache-kylin-standalone)
| Tag                  | Image Contents                                                            | Comment & Publish Date                                                                                                                                   |
|----------------------|---------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| 5.0.0-GA             | [**Recommended for users**] The official 5.0.0-GA with Spark & Gluten bundled. | Uploaded at 2024-09-13 |
| 5.0-beta             | The official 5.0.0-beta with Spark bundled.    | Uploaded at 2023-09-08, worked fine on Docker Desktop Mac 4.3.0 & 4.22.1(and Windows) ,                                                                  |
| kylin-4.0.1-mondrian | The official Kylin 4.0.1 with **MDX** function enabled                    | Uploaded at 2022-05-13                                                                                                                                   | 
| 5-dev                | [**For developer only**] Kylin 5.X package with some sample data/tools etc | Uploaded at 2023-11-21, this image for developer to debug and test Kylin 5.X source code if he/her didn't have a Hadoop env                              |
| 5.x-base-dev-only    | [**For maintainer only**] Hadoop, Hive, Zookeeper, MySQL, JDK8            | Uploaded at 2023-09-07, this is the base image for all Kylin 5.X image, so it didn't contain Kylin package, see file `Dockerfile_hadoop` for information |

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
    --hostname localhost \
    -e TZ=UTC \
    -m 10G \
    -p 7070:7070 \
    -p 8088:8088 \
    -p 9870:9870 \
    -p 8032:8032 \
    -p 8042:8042 \
    -p 2181:2181 \
    apachekylin/apache-kylin-standalone:5.0.0-GA

docker logs --follow Kylin5-Machine
```

When you enter these two commands, the logs will scroll
out in terminal and the process will continue for 3-5 minutes.

```
===============================================================================
*******************************************************************************
|
|   Start SSH server at Fri Sep 13 12:15:24 UTC 2024
|   Command: /etc/init.d/ssh start
|
 * Starting OpenBSD Secure Shell server sshd
   ...done.
[Start SSH server] succeed.

===============================================================================
*******************************************************************************
|
|   Start MySQL at Fri Sep 13 12:15:25 UTC 2024
|   Command: service mysql start
|
 * Starting MySQL database server mysqld
su: warning: cannot change directory to /nonexistent: No such file or directory
   ...done.
[Start MySQL] succeed.

===============================================================================
*******************************************************************************
|
|   Create Database kylin at Fri Sep 13 12:15:36 UTC 2024
|   Command: mysql -uroot -p123456 -e CREATE DATABASE IF NOT EXISTS kylin default charset utf8mb4 COLLATE utf8mb4_general_ci;
|
mysql: [Warning] Using a password on the command line interface can be insecure.
[Create Database kylin] succeed.

===============================================================================
*******************************************************************************
|
|   Create Database hive3 at Fri Sep 13 12:15:36 UTC 2024
|   Command: mysql -uroot -p123456 -e CREATE DATABASE IF NOT EXISTS hive3 default charset utf8mb4 COLLATE utf8mb4_general_ci;
|
mysql: [Warning] Using a password on the command line interface can be insecure.
[Create Database hive3] succeed.

===============================================================================
*******************************************************************************
|
|   Init Hive at Fri Sep 13 12:15:36 UTC 2024
|   Command: schematool -initSchema -dbType mysql
|
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/apache-hive-3.1.3-bin/lib/log4j-slf4j-impl-2.17.1.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop-3.2.4/share/hadoop/common/lib/slf4j-reload4j-1.7.35.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
Metastore connection URL: jdbc:mysql://127.0.0.1:3306/hive3?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8
Metastore Connection Driver : com.mysql.cj.jdbc.Driver
Metastore connection User: root
Starting metastore schema initialization to 3.1.0
Initialization script hive-schema-3.1.0.mysql.sql
...
Initialization script completed
schemaTool completed
[Init Hive] succeed.

===============================================================================
*******************************************************************************
|
|   Format HDFS at Fri Sep 13 12:15:50 UTC 2024
|   Command: hdfs namenode -format
|
WARNING: /opt/hadoop-3.2.4/logs does not exist. Creating.
2024-09-13 12:15:51,423 INFO namenode.NameNode: STARTUP_MSG: 
/************************************************************
STARTUP_MSG: Starting NameNode
STARTUP_MSG:   host = localhost/127.0.0.1
STARTUP_MSG:   args = [-format]
STARTUP_MSG:   version = 3.2.4
STARTUP_MSG:   classpath = /opt/hadoop-3.2.4/etc/hadoop:/opt/hadoop-3.2.4/share/hadoop/common/lib/audience-annotations-0.5.0.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/httpclient-4.5.13.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/curator-client-2.13.0.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jetty-server-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/checker-qual-2.5.2.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/woodstox-core-5.3.0.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/javax.activation-api-1.2.0.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jackson-jaxrs-1.9.13.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jersey-core-1.19.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/kerb-server-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jcip-annotations-1.0-1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/error_prone_annotations-2.2.0.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/accessors-smart-2.4.7.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jetty-util-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jetty-servlet-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/kerby-config-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jackson-core-asl-1.9.13.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/kerb-identity-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/commons-beanutils-1.9.4.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/j2objc-annotations-1.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jsp-api-2.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jersey-json-1.19.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/commons-lang3-3.7.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jersey-servlet-1.19.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/commons-io-2.8.0.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/kerb-core-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/asm-5.0.4.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/zookeeper-3.4.14.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/commons-compress-1.21.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/curator-recipes-2.13.0.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/netty-3.10.6.Final.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/paranamer-2.3.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/httpcore-4.4.13.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/commons-text-1.4.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jackson-annotations-2.10.5.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/commons-logging-1.1.3.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jackson-mapper-asl-1.9.13.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/kerby-util-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jetty-xml-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/failureaccess-1.0.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/slf4j-reload4j-1.7.35.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/animal-sniffer-annotations-1.17.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jetty-http-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jackson-databind-2.10.5.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/slf4j-api-1.7.35.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/commons-collections-3.2.2.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/avro-1.7.7.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/token-provider-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/nimbus-jose-jwt-9.8.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jsch-0.1.55.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jaxb-api-2.2.11.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/commons-configuration2-2.1.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/hadoop-annotations-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/commons-codec-1.11.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/kerby-pkix-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jsr311-api-1.1.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jul-to-slf4j-1.7.35.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jetty-io-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/kerb-simplekdc-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/curator-framework-2.13.0.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/stax2-api-4.2.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jackson-core-2.10.5.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/spotbugs-annotations-3.1.9.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/kerb-admin-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/reload4j-1.2.18.3.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/htrace-core4-4.1.0-incubating.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/re2j-1.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/metrics-core-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jersey-server-1.19.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/protobuf-java-2.5.0.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jackson-xc-1.9.13.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/kerb-util-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/kerb-common-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jsr305-3.0.2.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jettison-1.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/json-smart-2.4.7.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/guava-27.0-jre.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/commons-cli-1.2.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/gson-2.9.0.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jetty-webapp-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/kerb-client-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jetty-security-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/javax.servlet-api-3.1.0.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/snappy-java-1.0.5.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/kerb-crypto-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jetty-util-ajax-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/commons-math3-3.1.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/hadoop-auth-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/jaxb-impl-2.2.3-1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/kerby-asn1-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/dnsjava-2.1.7.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/kerby-xdr-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/common/lib/commons-net-3.6.jar:/opt/hadoop-3.2.4/share/hadoop/common/hadoop-common-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/common/hadoop-common-3.2.4-tests.jar:/opt/hadoop-3.2.4/share/hadoop/common/hadoop-kms-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/common/hadoop-nfs-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/commons-daemon-1.0.13.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/audience-annotations-0.5.0.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/httpclient-4.5.13.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/curator-client-2.13.0.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jetty-server-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/checker-qual-2.5.2.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/woodstox-core-5.3.0.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/javax.activation-api-1.2.0.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jackson-jaxrs-1.9.13.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jersey-core-1.19.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/kerb-server-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jcip-annotations-1.0-1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/error_prone_annotations-2.2.0.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/accessors-smart-2.4.7.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jetty-util-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jetty-servlet-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/netty-all-4.1.68.Final.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/kerby-config-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/okio-1.6.0.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jackson-core-asl-1.9.13.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/kerb-identity-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/commons-beanutils-1.9.4.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/j2objc-annotations-1.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jersey-json-1.19.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/commons-lang3-3.7.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jersey-servlet-1.19.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/commons-io-2.8.0.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/kerb-core-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/asm-5.0.4.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/zookeeper-3.4.14.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/commons-compress-1.21.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/curator-recipes-2.13.0.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/netty-3.10.6.Final.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/paranamer-2.3.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/httpcore-4.4.13.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/commons-text-1.4.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/okhttp-2.7.5.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jackson-annotations-2.10.5.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/commons-logging-1.1.3.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jackson-mapper-asl-1.9.13.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/kerby-util-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jetty-xml-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/failureaccess-1.0.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/animal-sniffer-annotations-1.17.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jetty-http-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jackson-databind-2.10.5.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/commons-collections-3.2.2.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/avro-1.7.7.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/token-provider-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/nimbus-jose-jwt-9.8.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jsch-0.1.55.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jaxb-api-2.2.11.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/commons-configuration2-2.1.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/hadoop-annotations-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/commons-codec-1.11.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/kerby-pkix-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jsr311-api-1.1.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jetty-io-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/kerb-simplekdc-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/curator-framework-2.13.0.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/stax2-api-4.2.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jackson-core-2.10.5.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/spotbugs-annotations-3.1.9.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/kerb-admin-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/reload4j-1.2.18.3.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/htrace-core4-4.1.0-incubating.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/leveldbjni-all-1.8.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/re2j-1.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jersey-server-1.19.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/protobuf-java-2.5.0.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jackson-xc-1.9.13.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/kerb-util-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/kerb-common-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jsr305-3.0.2.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jettison-1.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/json-smart-2.4.7.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/guava-27.0-jre.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/commons-cli-1.2.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/gson-2.9.0.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jetty-webapp-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/kerb-client-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jetty-security-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/javax.servlet-api-3.1.0.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/snappy-java-1.0.5.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/kerb-crypto-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jetty-util-ajax-9.4.43.v20210629.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/commons-math3-3.1.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/hadoop-auth-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/jaxb-impl-2.2.3-1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/json-simple-1.1.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/kerby-asn1-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/dnsjava-2.1.7.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/kerby-xdr-1.0.1.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/lib/commons-net-3.6.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/hadoop-hdfs-rbf-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/hadoop-hdfs-3.2.4-tests.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/hadoop-hdfs-nfs-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/hadoop-hdfs-client-3.2.4-tests.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/hadoop-hdfs-native-client-3.2.4-tests.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/hadoop-hdfs-client-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/hadoop-hdfs-rbf-3.2.4-tests.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/hadoop-hdfs-httpfs-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/hadoop-hdfs-native-client-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/hdfs/hadoop-hdfs-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/mapreduce/lib/junit-4.13.2.jar:/opt/hadoop-3.2.4/share/hadoop/mapreduce/lib/hamcrest-core-1.3.jar:/opt/hadoop-3.2.4/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/mapreduce/hadoop-mapreduce-client-shuffle-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/mapreduce/hadoop-mapreduce-client-app-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/mapreduce/hadoop-mapreduce-client-common-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-plugins-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/mapreduce/hadoop-mapreduce-client-nativetask-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/mapreduce/hadoop-mapreduce-client-uploader-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.4-tests.jar:/opt/hadoop-3.2.4/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/guice-4.0.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/swagger-annotations-1.5.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/jakarta.xml.bind-api-2.3.2.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/java-util-1.9.0.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/ehcache-3.3.1.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/snakeyaml-1.26.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/HikariCP-java7-2.4.12.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/jersey-guice-1.19.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/jackson-jaxrs-base-2.10.5.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/mssql-jdbc-6.2.1.jre7.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/aopalliance-1.0.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/javax.inject-1.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/geronimo-jcache_1.0_spec-1.0-alpha-1.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/fst-2.50.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/jackson-module-jaxb-annotations-2.10.5.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/metrics-core-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/jakarta.activation-api-1.2.1.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/bcprov-jdk15on-1.60.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/guice-servlet-4.0.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/jersey-client-1.19.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/json-io-2.5.1.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/jackson-jaxrs-json-provider-2.10.5.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/bcpkix-jdk15on-1.60.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/lib/objenesis-1.0.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-services-core-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-server-common-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-api-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.2.4.jar:/opt/hadoop-3.2.4/share/had2024-09-13T12:15:51.426296551Z oop/yarn/hadoop-yarn-registry-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-server-router-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-common-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-server-timeline-pluginstorage-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-server-applicationhistoryservice-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-server-tests-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-server-resourcemanager-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-server-sharedcachemanager-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-server-web-proxy-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-server-nodemanager-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-client-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-submarine-3.2.4.jar:/opt/hadoop-3.2.4/share/hadoop/yarn/hadoop-yarn-services-api-3.2.4.jar
STARTUP_MSG:   build = Unknown -r 7e5d9983b388e372fe640f21f048f2f2ae6e9eba; compiled by 'ubuntu' on 2022-07-12T11:58Z
STARTUP_MSG:   java = 1.8.0_422
************************************************************/
2024-09-13 12:15:51,434 INFO namenode.NameNode: registered UNIX signal handlers for [TERM, HUP, INT]
2024-09-13 12:15:51,539 INFO namenode.NameNode: createNameNode [-format]
Formatting using clusterid: CID-e9f0293c-adcd-40a6-9c7f-ab7537b2eedf
2024-09-13 12:15:52,059 INFO namenode.FSEditLog: Edit logging is async:true
2024-09-13 12:15:52,090 INFO namenode.FSNamesystem: KeyProvider: null
2024-09-13 12:15:52,092 INFO namenode.FSNamesystem: fsLock is fair: true
2024-09-13 12:15:52,092 INFO namenode.FSNamesystem: Detailed lock hold time metrics enabled: false
2024-09-13 12:15:52,100 INFO namenode.FSNamesystem: fsOwner             = root (auth:SIMPLE)
2024-09-13 12:15:52,100 INFO namenode.FSNamesystem: supergroup          = supergroup
2024-09-13 12:15:52,100 INFO namenode.FSNamesystem: isPermissionEnabled = true
2024-09-13 12:15:52,100 INFO namenode.FSNamesystem: HA Enabled: false
2024-09-13 12:15:52,153 INFO common.Util: dfs.datanode.fileio.profiling.sampling.percentage set to 0. Disabling file IO profiling
2024-09-13 12:15:52,165 INFO blockmanagement.DatanodeManager: dfs.block.invalidate.limit: configured=1000, counted=60, effected=1000
2024-09-13 12:15:52,165 INFO blockmanagement.DatanodeManager: dfs.namenode.datanode.registration.ip-hostname-check=true
2024-09-13 12:15:52,169 INFO blockmanagement.BlockManager: dfs.namenode.startup.delay.block.deletion.sec is set to 000:00:00:00.000
2024-09-13 12:15:52,169 INFO blockmanagement.BlockManager: The block deletion will start around 2024 Sep 13 12:15:52
2024-09-13 12:15:52,171 INFO util.GSet: Computing capacity for map BlocksMap
2024-09-13 12:15:52,171 INFO util.GSet: VM type       = 64-bit
2024-09-13 12:15:52,172 INFO util.GSet: 2.0% max memory 1.7 GB = 34.8 MB
2024-09-13 12:15:52,172 INFO util.GSet: capacity      = 2^22 = 4194304 entries
2024-09-13 12:15:52,180 INFO blockmanagement.BlockManager: Storage policy satisfier is disabled
2024-09-13 12:15:52,180 INFO blockmanagement.BlockManager: dfs.block.access.token.enable = false
2024-09-13 12:15:52,186 INFO blockmanagement.BlockManagerSafeMode: dfs.namenode.safemode.threshold-pct = 0.9990000128746033
2024-09-13 12:15:52,186 INFO blockmanagement.BlockManagerSafeMode: dfs.namenode.safemode.min.datanodes = 0
2024-09-13 12:15:52,186 INFO blockmanagement.BlockManagerSafeMode: dfs.namenode.safemode.extension = 30000
2024-09-13 12:15:52,187 INFO blockmanagement.BlockManager: defaultReplication         = 1
2024-09-13 12:15:52,187 INFO blockmanagement.BlockManager: maxReplication             = 512
2024-09-13 12:15:52,187 INFO blockmanagement.BlockManager: minReplication             = 1
2024-09-13 12:15:52,187 INFO blockmanagement.BlockManager: maxReplicationStreams      = 2
2024-09-13 12:15:52,187 INFO blockmanagement.BlockManager: redundancyRecheckInterval  = 3000ms
2024-09-13 12:15:52,187 INFO blockmanagement.BlockManager: encryptDataTransfer        = false
2024-09-13 12:15:52,188 INFO blockmanagement.BlockManager: maxNumBlocksToLog          = 1000
2024-09-13 12:15:52,238 INFO namenode.FSDirectory: GLOBAL serial map: bits=29 maxEntries=536870911
2024-09-13 12:15:52,238 INFO namenode.FSDirectory: USER serial map: bits=24 maxEntries=16777215
2024-09-13 12:15:52,238 INFO namenode.FSDirectory: GROUP serial map: bits=24 maxEntries=16777215
2024-09-13 12:15:52,238 INFO namenode.FSDirectory: XATTR serial map: bits=24 maxEntries=16777215
2024-09-13 12:15:52,259 INFO util.GSet: Computing capacity for map INodeMap
2024-09-13 12:15:52,259 INFO util.GSet: VM type       = 64-bit
2024-09-13 12:15:52,259 INFO util.GSet: 1.0% max memory 1.7 GB = 17.4 MB
2024-09-13 12:15:52,259 INFO util.GSet: capacity      = 2^21 = 2097152 entries
2024-09-13 12:15:52,260 INFO namenode.FSDirectory: ACLs enabled? false
2024-09-13 12:15:52,260 INFO namenode.FSDirectory: POSIX ACL inheritance enabled? true
2024-09-13 12:15:52,260 INFO namenode.FSDirectory: XAttrs enabled? true
2024-09-13 12:15:52,261 INFO namenode.NameNode: Caching file names occurring more than 10 times
2024-09-13 12:15:52,265 INFO snapshot.SnapshotManager: Loaded config captureOpenFiles: false, skipCaptureAccessTimeOnlyChange: false, snapshotDiffAllowSnapRootDescendant: true, maxSnapshotLimit: 65536
2024-09-13 12:15:52,267 INFO snapshot.SnapshotManager: SkipList is disabled
2024-09-13 12:15:52,272 INFO util.GSet: Computing capacity for map cachedBlocks
2024-09-13 12:15:52,272 INFO util.GSet: VM type       = 64-bit
2024-09-13 12:15:52,272 INFO util.GSet: 0.25% max memory 1.7 GB = 4.4 MB
2024-09-13 12:15:52,272 INFO util.GSet: capacity      = 2^19 = 524288 entries
2024-09-13 12:15:52,284 INFO metrics.TopMetrics: NNTop conf: dfs.namenode.top.window.num.buckets = 10
2024-09-13 12:15:52,284 INFO metrics.TopMetrics: NNTop conf: dfs.namenode.top.num.users = 10
2024-09-13 12:15:52,284 INFO metrics.TopMetrics: NNTop conf: dfs.namenode.top.windows.minutes = 1,5,25
2024-09-13 12:15:52,288 INFO namenode.FSNamesystem: Retry cache on namenode is enabled
2024-09-13 12:15:52,288 INFO namenode.FSNamesystem: Retry cache will use 0.03 of total heap and retry cache entry expiry time is 600000 millis
2024-09-13 12:15:52,291 INFO util.GSet: Computing capacity for map NameNodeRetryCache
2024-09-13 12:15:52,291 INFO util.GSet: VM type       = 64-bit
2024-09-13 12:15:52,292 INFO util.GSet: 0.029999999329447746% max memory 1.7 GB = 535.3 KB
2024-09-13 12:15:52,292 INFO util.GSet: capacity      = 2^16 = 65536 entries
2024-09-13 12:15:52,314 INFO namenode.FSImage: Allocated new BlockPoolId: BP-1031271309-127.0.0.1-1726229752306
2024-09-13 12:15:52,328 INFO common.Storage: Storage directory /data/hadoop/dfs/name has been successfully formatted.
2024-09-13 12:15:52,352 INFO namenode.FSImageFormatProtobuf: Saving image file /data/hadoop/dfs/name/current/fsimage.ckpt_0000000000000000000 using no compression
2024-09-13 12:15:52,435 INFO namenode.FSImageFormatProtobuf: Image file /data/hadoop/dfs/name/current/fsimage.ckpt_0000000000000000000 of size 396 bytes saved in 0 seconds .
2024-09-13 12:15:52,447 INFO namenode.NNStorageRetentionManager: Going to retain 1 images with txid >= 0
2024-09-13 12:15:52,470 INFO namenode.FSNamesystem: Stopping services started for active state
2024-09-13 12:15:52,470 INFO namenode.FSNamesystem: Stopping services started for standby state
2024-09-13 12:15:52,474 INFO namenode.FSImage: FSImageSaver clean checkpoint: txid=0 when meet shutdown.
2024-09-13 12:15:52,475 INFO namenode.NameNode: SHUTDOWN_MSG: 
/************************************************************
SHUTDOWN_MSG: Shutting down NameNode at localhost/127.0.0.1
************************************************************/
[Format HDFS] succeed.

===============================================================================
*******************************************************************************
|
|   Start Zookeeper at Fri Sep 13 12:15:52 UTC 2024
|   Command: /opt/apache-zookeeper-3.7.2-bin/bin/zkServer.sh start
|
ZooKeeper JMX enabled by default
Using config: /opt/apache-zookeeper-3.7.2-bin/bin/../conf/zoo.cfg
Starting zookeeper ... STARTED
[Start Zookeeper] succeed.

===============================================================================
*******************************************************************************
|
|   Start Hadoop at Fri Sep 13 12:15:53 UTC 2024
|   Command: /opt/hadoop-3.2.4/sbin/start-all.sh
|
Starting namenodes on [localhost]
localhost: Warning: Permanently added 'localhost' (ED25519) to the list of known hosts.
Starting datanodes
Starting secondary namenodes [localhost]
Starting resourcemanager
Starting nodemanagers
[Start Hadoop] succeed.

===============================================================================
*******************************************************************************
|
|   Start History Server at Fri Sep 13 12:16:09 UTC 2024
|   Command: /opt/hadoop-3.2.4/sbin/start-historyserver.sh
|
WARNING: Use of this script to start the MR JobHistory daemon is deprecated.
WARNING: Attempting to execute replacement "mapred --daemon start" instead.
[Start History Server] succeed.

===============================================================================
*******************************************************************************
|
|   Start Hive metastore at Fri Sep 13 12:16:11 UTC 2024
|   Command: /opt/apache-hive-3.1.3-bin/bin/start-hivemetastore.sh
|
[Start Hive metastore] succeed.
Checking Check Hive metastore's status...
+
Check Check Hive metastore succeed.

===============================================================================
*******************************************************************************
|
|   Start Hive server at Fri Sep 13 12:16:22 UTC 2024
|   Command: /opt/apache-hive-3.1.3-bin/bin/start-hiveserver2.sh
|
[Start Hive server] succeed.
Checking Check Hive server's status...
+
Check Check Hive server succeed.

===============================================================================
*******************************************************************************
|
|   Prepare sample data at Fri Sep 13 12:16:45 UTC 2024
|   Command: /home/kylin/apache-kylin-5.0.0-GA-bin/bin/sample.sh
|
Loading sample data into HDFS tmp path: /tmp/kylin/sample_cube/data
WARNING: log4j.properties is not found. HADOOP_CONF_DIR may be incomplete.
WARNING: log4j.properties is not found. HADOOP_CONF_DIR may be incomplete.
Going to create sample tables in hive to database SSB by hive
WARNING: log4j.properties is not found. HADOOP_CONF_DIR may be incomplete.
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/apache-hive-3.1.3-bin/lib/log4j-slf4j-impl-2.17.1.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop-3.2.4/share/hadoop/common/lib/slf4j-reload4j-1.7.35.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
Hive Session ID = 2e70c349-7575-4a00-84c8-08b24f1a38cb

Logging initialized using configuration in jar:file:/opt/apache-hive-3.1.3-bin/lib/hive-common-3.1.3.jar!/hive-log4j2.properties Async: true
Hive Session ID = bc35a6b2-1846-4a03-837f-271679ac6185
OK
Time taken: 1.136 seconds
WARNING: log4j.properties is not found. HADOOP_CONF_DIR may be incomplete.
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/apache-hive-3.1.3-bin/lib/log4j-slf4j-impl-2.17.1.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop-3.2.4/share/hadoop/common/lib/slf4j-reload4j-1.7.35.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
Hive Session ID = 2e1f6b0e-4017-4e44-871d-239ab6dadc29

Logging initialized using configuration in jar:file:/opt/apache-hive-3.1.3-bin/lib/hive-common-3.1.3.jar!/hive-log4j2.properties Async: true
Hive Session ID = 965d433f-9635-4bfb-9aa6-93705ab733a4
...
Time taken: 1.946 seconds
Loading data to table ssb.customer
OK
Time taken: 0.517 seconds
Loading data to table ssb.dates
OK
Time taken: 0.256 seconds
Loading data to table ssb.lineorder
OK
Time taken: 0.248 seconds
Loading data to table ssb.part
OK
Time taken: 0.254 seconds
Loading data to table ssb.supplier
OK
Time taken: 0.243 seconds
Sample hive tables are created successfully; Going to create sample project...
kylin version is 5.0.0.0
The metadata backup path is hdfs://localhost:9000/kylin/kylin/_backup/2024-09-13-12-17-29_backup/core_meta.
Sample model is created successfully in project 'learn_kylin'. Detailed Message is at "logs/shell.stderr".
[Prepare sample data] succeed.

===============================================================================
*******************************************************************************
|
|   Kylin ENV bypass at Fri Sep 13 12:17:29 UTC 2024
|   Command: touch /home/kylin/apache-kylin-5.0.0-GA-bin/bin/check-env-bypass
|
[Kylin ENV bypass] succeed.

===============================================================================
*******************************************************************************
|
|   Start Kylin Instance at Fri Sep 13 12:17:29 UTC 2024
|   Command: /home/kylin/apache-kylin-5.0.0-GA-bin/bin/kylin.sh -v start
|
java is /usr/lib/jvm/java-8-openjdk-amd64/bin/java
Starting Kylin...
This user don't have permission to run crontab.
KYLIN_HOME is:/home/kylin/apache-kylin-5.0.0-GA-bin
KYLIN_CONFIG_FILE is:/home/kylin/apache-kylin-5.0.0-GA-bin/conf/kylin.properties
SPARK_HOME is:/home/kylin/apache-kylin-5.0.0-GA-bin/spark
Retrieving hadoop config dir...
KYLIN_JVM_SETTINGS is -server -Xms1g -Xmx8g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=16m -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark  -Xloggc:/home/kylin/apache-kylin-5.0.0-GA-bin/logs/kylin.gc.%p  -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=64M -XX:-OmitStackTraceInFastThrow -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector -DAsyncLogger.RingBufferSize=8192
KYLIN_DEBUG_SETTINGS is not set, will not enable remote debuging
KYLIN_LD_LIBRARY_SETTINGS is not set, it is okay unless you want to specify your own native path
SPARK_HDP_VERSION is set to 'hadoop'
Export SPARK_HOME to /home/kylin/apache-kylin-5.0.0-GA-bin/spark
LD_PRELOAD= is:/home/kylin/apache-kylin-5.0.0-GA-bin/server/libch.so
Checking Zookeeper role...
Checking Spark directory...
Kylin is starting. It may take a while. For status, please visit http://localhost:7070/kylin/index.html.
You may also check status via: PID:4746, or Log: /home/kylin/apache-kylin-5.0.0-GA-bin/logs/kylin.log.
[Start Kylin Instance] succeed.
Checking Check Env Script's status...
/home/kylin/apache-kylin-5.0.0-GA-bin/bin/check-env-bypass
+
Check Check Env Script succeed.
Checking Kylin Instance's status...
...
Check Kylin Instance succeed.
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

If you are interested in `Dockerfile`, please visit https://github.com/apache/kylin/blob/kylin5/dev-support/release-manager/standalone-docker/all-in-one/Dockerfile .

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
