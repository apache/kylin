---
layout: docs23
title:  Setup JDBC Data Source
categories: howto
permalink: /docs23/howto/howto_setup_jdbc_datasource.html
---

> Available since Apache Kylin v2.3.x

## Supported JDBC data source

Since v2.3.0 Apache Kylin starts to support JDBC as the third type of data source (after Hive, Kafka). User can integrate Kylin with their SQL database or data warehouses like MySQL, Microsoft SQL Server and HP Vertica directly. Other relational databases are easy to support as well.

## Configure JDBC data source

1. Prepare Sqoop

Kylin uses Apache Sqoop to load data from relational databases to HDFS. Download and install the latest version of Sqoop in the same machine with Kylin. We will use `SQOOP_HOME` environment variable to indicate the installation directory of Sqoop in this guide.

2. Prepare JDBC driver

   Copy Kylin JDBC driver from `$KYLIN_HOME/lib` to `$SQOOP_HOME/lib`.

3. Configure Kylin

In `$KYLIN_HOME/conf/kylin.properties`, add the following configurations (assumming your MySQL is in the same host):

```
kylin.source.default=8
kylin.source.jdbc.connection-url=jdbc:mysql://localhost:3306/employees
kylin.source.jdbc.driver=com.mysql.jdbc.Driver
kylin.source.jdbc.dialect=mysql
kylin.source.jdbc.user=your_mysql_username
kylin.source.jdbc.pass=your_mysql_password
kylin.source.jdbc.sqoop-home=/usr/hdp/current/sqoop-client/bin
kylin.source.jdbc.filed-delimiter=|
```

Please note, when configure these parameters in `conf/kylin.properties`, all your projects are using the JDBC as data source. If you need access different types of data source, you need configure these parameters at project level, this is the recommended way.

## Load tables from JDBC data source

Restart Kylin to make the changes effective. You can load tables from JDBC data source now. Visit Kylin web and then navigate to the data source panel. 

Click **Load table** button and then input table' name, or click the "Load Table From Tree" button and select the tables to load. Uncheck **Calculate column cardinality** because this feature is not supported for JDBC data source.

Click "Sync", Kylin will load the tables' definition through the JDBC interface. You can see tables and columns when tables are loaded successfully, similar as Hive.

![](/images/docs/jdbc-datasource/load_table_03.png)

Go ahead and design your model and Cube. When building the Cube, Kylin will use Sqoop to import the data to HDFS, and then run building over it.