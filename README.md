Kylin OLAP
===========


Overview
------------
> Extreme OLAP Engine for Big Data

An open source distributed Analytical Engine from eBay to provide SQL interface and multi-dimensional analysis (OLAP) on Hadoop to support TB to PB size analysis.

Architecture
------------



Getting Started 
---------------

### Prerequisites ###

#### Hadoop ####
- Hadoop: 2.2.0.2.0.6.0-61 or above
- Hive: 0.12.0.2.0.6.0-61 or above
- HBase: 0.96.0.2.0.6.0-61-hadoop2

#### Kylin Server ####
- Command hadoop, hive, hbase is workable on your hadoop cluster
- JDK Runtime: JDK7 (OpenJDK or Oracle JDK)
- Maven
- Git
- Tomcat
- Mysql

#### Before Install ####
- Mysql: mysql is running and user "root@localhost" without password is ready.
- For the quick tutorial, we assume that your hadoop has priviledges disabled which means any user could run hadoop and hive command
- CATALINA_HOME is set

### Building Kylin ###
To compile Kylin from source code, clone from this repository and run:

    mvn clean compile

Kylin will run as web server for user to interactive with it, to generate deployment package (war file) for host on J2EE application server:

    mvn clean package

Kylin has a comprehensive set of unit tests that can take long time to run especially for cube generation. You can disable the tests when building:

    mvn clean package -DskipTests


### Run Kylin ###
To run Kylin, deploy geneated war file into J2EE application server like Tomcat, and restart application server. then access from browser via following URL:
[http://\<localhost>:\<port>/\<application name>]


### Resources ###

* Google Group:  [Kylin OLAP Group](https://groups.google.com/forum/#!forum/kylin-dev)

* Mail list: [kylin-dev@googlegroups.com](mailto:kylin-dev@googlegroups.com)


