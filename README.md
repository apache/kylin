Kylin OLAP
===========


Overview
------------
> Extreme OLAP Engine for Big Data

An open source distributed Analytical Engine from eBay to provide SQL interface and multi-dimensional analysis (OLAP) on Hadoop to support TB to PB size analysis.


Getting Started 
---------------

### Prerequisites ###

#### Hadoop ####
- Hadoop: 2.2.0.2.0.6.0-61 or above
- Hive: 0.12.0.2.0.6.0-61 or above
- HBase: 0.96.0.2.0.6.0-61-hadoop2


_Tested with Hortonworks HDP 2.1.3 and Cloudera Quickstart VM 5.1._

#### Misc ####
- Command hadoop, hive, hbase is workable on your hadoop cluster
- JDK Runtime: JDK7 (OpenJDK or Oracle JDK)
- Maven
- Git
- Tomcat (CATALINA_HOME being set)
- Npm

#### Before Install ####
- For the quick tutorial, we assume that your hadoop has priviledges disabled which means any user could run hadoop and hive command


### Installation ###

It is very easy to install Kylin for exploration/development. There are 3 possible ways:

1. Quick Start (Docker)
2. Sandbox     (HDP or CDH sandbox)
3. Dev Environment (IDE + Sandbox)

#### Quick Start using Docker ####


#### Sandbox (On-Hadoop-CLI installation) ####

If you are free to install Kylin on your hadoop CLI machine or Hadoop sandbox, this is the most convenient scenario, for it puts everything in a single machine.



![On-Hadoop-CLI-installation](https://github.com/KylinOLAP/kylinolap.github.io/blob/master/docs/installation/Picture1.png)

For normal users, the application in the above picture means `Kylin Web`, which contains a web interface for cube building, querying and all sorts of management. Kylin Web launches a query engine for querying and a cube build engine for building cubes. These two engines interact with the components in Hadoop CLI, like hive and hbase.

For a hands-on tutorial please visit [On-Hadoop-CLI installation](https://github.com/KylinOLAP/Kylin/wiki/On-Hadoop-CLI-installation).

#### Dev Environment (Off-Hadoop-CLI Installation) ####

This is typically for development environment setup.

Applications does not necessarily mean Kylin Web, it can also be any test cases or java applications running at your local IDE(query engine and cube build engine can be launched programmatically in your code). In this case, it is no longer reasonable to assume that all the Kylin components reside in the same machine as your Hadoop CLI.  Fortunately, Kylin still works under such condition with proper settings on your CLI machine.

![Off-CLI Installation](https://github.com/KylinOLAP/kylinolap.github.io/blob/master/docs/installation/Picture2.png)

For a hands-on tutorial please visit [Off-Hadoop-CLI installation](https://github.com/KylinOLAP/Kylin/wiki/Off-Hadoop-CLI-Installation-(Dev-Env-Setup))


### Resources ###

* Web Site: <http://kylin.io>

* Google Group:  [Kylin OLAP Group](https://groups.google.com/forum/#!forum/kylin-olap)

* Developer Mail: <kylin-olap@googlegroups.com>

* How To Contribute: See [wiki](https://github.com/KylinOLAP/Kylin/wiki/How-to-Contribute)

* Presentation: [Kylin Hadoop OLAP Engine v1.0](https://github.com/KylinOLAP/Kylin/blob/master/docs/Kylin_Hadoop_OLAP_Engine_v1.0.pdf?raw=true)

