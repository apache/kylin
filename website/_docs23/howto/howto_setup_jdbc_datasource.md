---
layout: docs23
title:  Set Up JDBC Data Source
categories: howto
permalink: /docs23/howto/howto_setup_jdbc_datasource.html
---

> Available since Apache Kylin v2.3.x

## Supported JDBC data source

Since v2.3.0 Apache Kylin has supported JDBC data source. Users can integrate Kylin with MySQL, Microsoft SQL Server and vertica directly. Other relational databases are not offically supported, but could be easily extended.

## Set Up JDBC data source

1. Prepare Sqoop

Kylin uses [Sqoop](http://sqoop.apache.org/) to load data from relational databases to HDFS. Make sure Sqoop is installed in the same node with Kylin. we will use SQOOP_HOME to indicate the installation directory of Sqoop in this guide.

2. Prepare JDBC driver

   Add appropriate JDBC driver to *$SQOOP_HOME/lib* and *$KYLIN_HOME/lib*.

3. Set Kylin configuration

For example:

```
kylin.source.default=8
kylin.source.jdbc.connection-url=jdbc:mysql://localhost:3306/employees
kylin.source.jdbc.driver=com.mysql.jdbc.Driver
kylin.source.jdbc.dialect=mysql
kylin.source.jdbc.user=root
kylin.source.jdbc.pass=my-secret-pw
kylin.source.jdbc.sqoop-home=/usr/hdp/current/sqoop-client/bin
kylin.source.jdbc.filed-delimiter=|
```

## Load tables from JDBC data source

If everything goes smoothly, you are able to load tables from JDBC data source now. Open Kylin and navigate to data source panel. 

![](/images/docs/v23/jdbc-datasource/load_table_01.png)

Click **load table** button and input tables' names. Do not click **Calculate column cardinality** because this feature has not been supported for JDBC data source.

![](/images/docs/v23/jdbc-datasource/load_table_02.png)

You can see tables and columns when tables are loaded successfully.

![](/images/docs/v23/jdbc-datasource/load_table_03.png)

And Kylin also provides a convenient way to select tables you want to load. Click **Load Table From Tree** then all tables are shown as a tree.

![](/images/docs/v23/jdbc-datasource/load_tables_tree_01.png)

Choose tables you want to sync with Kylin and unclick **Calculate column cardinality**.

![](/images/docs/v23/jdbc-datasource/load_tables_tree_02.png)

Go ahead and design your models and cubes.