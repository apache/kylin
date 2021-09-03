---
layout: docs31
title:  Use MySQL as Metastore
categories: tutorial
permalink: /docs31/tutorial/mysql_metastore.html
since: v2.5.0
---

Kylin supports MySQL as Metastore.



### Prerequisites

1.Install MySQL, such as v5.1.17
2.Download the MySQL JDBC driver ( `mysql-connector-java-<version>.jar`) and place it in the `$KYLIN_HOME/ext/` directory.

> *Note*: Please create it yourself, if you do not have this directory.



### Configuration Steps

1.Create a new database in MySQL for storing Kylin metadata, such as `kylin`

2.Configure `kylin.metadata.url={metadata_name}@jdbc` in the configuration file `kylin.properties`. 
The description of each configuration item in this parameter is as follows, where `url`, `username` and `password` are required.

> Note: {metadata_name} needs to be replaced with the metadata table name, if the table already exists, the existing table will be used; if it does not exist, the table will be created automatically.

- `url`: URL of the JDBC connection
- `username`: JDBC username
- `password`: JDBC password, if the password is encrypted, fill in the encrypted password
- `driverClassName`: JDBC driver class name. The default value is *com.mysql.jdbc.Driver*
- `maxActive`: Maximum number of database connections. The default value is 5
- `maxIdle`: Maximum number of connections waiting. The default value is 5
- `maxWait`: Maximum number of milliseconds to wait for connection. The default value is 1000.
- `removeAbandoned`: Whether to automatically reclaim timeout connections. The default value is *TRUE*
- `removeAbandonedTimeout`: timeout seconds. The default value is 300
- `passwordEncrypted`: Whether to encrypt the JDBC password. The default value is *FALSE*

> Note: To encrypt the JDBC password, run the following command under `$KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/lib/`:

```sh
java -classpath kylin-server-base-<version>.jar\
:kylin-core-common-<version>.jar\
:spring-beans-4.3.10.RELEASE.jar:spring-core-4.3.10.RELEASE.jar\
:commons-codec-1.7.jar \
org.apache.kylin.rest.security.PasswordPlaceholderConfigurer \
AES <your_password>
```

As in Kylin v2.5, execute the following command:

```sh
java -classpath kylin-server-base-2.5.0.jar\
:kylin-core-common-2.5.0.jar\
:spring-beans-4.3.10.RELEASE.jar\
:spring-core-4.3.10.RELEASE.jar\
:commons-codec-1.7.jar \
org.apache.kylin.rest.security.PasswordPlaceholderConfigurer \
AES test123
```

The execution output is as belows:

```
AES encrypted password is:
bUmSqT/opyqz89Geu0yQ3g==
```

Fill in the generated password into `password` in `kylin.metadata.url` and set `passwordEncrypted` to *TRUE*.

3.Since the metadata does not depend on HBase, you need to add the ZooKeeper connection configuration `kylin.env.zookeeper-connect-string = host:port` in the configuration file `kylin.properties`.

4.The sample configuration of `kylin.properties` is as follows:

```properties
Kylin.metadata.url=mysql_test@jdbc,url=jdbc:mysql://localhost:3306/kylin,username=kylin_test,password=bUmSqT/opyqz89Geu0yQ3g==,maxActive=10,maxIdle=10,passwordEncrypted=true
Kylin.env.zookeeper-connect-string=localhost:2181
```

5.Start Kylin