---
layout: post-blog
title:  Connecting Tableau Desktop and Tableau Server with Apache Kylin
date:   2019-09-22 20:30:00
author: Piotr Naszarkowski
categories: blog
---

## Background 

This document describes how to connect Tableau to Apache Kylin OLAP server, particularly (but not only) in live mode to use both reporting and analytics features of Tableau together with Apache Kylin's fast query processing engine. The configuration is platform independent - it works for both Windows and Linux installations of Tableau Server.

For the time of writing this guide we tested that it works with Kylin 3.0.0 and Tableau Server 2019.1.  

## Prerequisites

### Apache Kylin JDBC Driver

First we need to get Apache Kylin JDBC Driver - kylin-jdbc-X.Y.Z.jar file. You can either get it from the compiled package available on the download page http://kylin.apache.org/download/ from `lib` folder or compile it on your own using instructions below.

*Note*: To make JDBC driver work properly, there has been a fix recently https://github.com/apache/kylin/pull/739 that upgraded one of the libraries used by the driver. The fix was applied for version 3, so if for some reason you need a jar for earlier version, you have to apply the fix on the lower version's codebase and compile yourself.

#### Compiling Apache Kylin JDBC Driver

```
git clone https://github.com/apache/kylin.git 
cd kylin
mvn clean package -DskipTests -am -pl jdbc
```

The compiled jar is located in the following location: `jdbc/target/kylin-jdbc-X.Y.Z.jar`

### Tableau Server on Linux

If you have installed Tableau Server in a Linux box, e.g. CentOS, copy the driver's jar file to the following location: `/opt/tableau/tableau_driver/jdbc/` and restart Tableau Server. 
The server is now ready to create and refresh data from Apache Kylin.

### Tableau Server and Tableau Desktop on Windows

For either Tableau Server or Tableau Desktop that is installed on a Windows machine, copy the driver's jar file to the following location `C:\Program Files\Tableau\Drivers` and restart Tableau Server or reopen Tableau Desktop.

Some more details regarding jdbc connection from Tableau are well described in Tableau's documentation: https://onlinehelp.tableau.com/current/pro/desktop/en-us/examples_otherdatabases_jdbc.htm.

## Creating report in Tableau Desktop - connecting to Apache Kylin

To create report follow the steps:
1. Open Tableau Desktop
2. Use "Other Databases (JDBC)" to create connection for the data source
![Other Databases (JDBC)](/images/blog/kylin-tableau/tableau_other_databases_jdbc.jpg)
3. Configure the connection in the following way:
- URL: `jdbc:kylin://<kylin-server-name>:<kylin-port>/<project>`
- Dialect: `SQL92`
![Datasource connection](/images/blog/kylin-tableau/tableau_kylin_connection.jpg)
4. Configure data source as follows:
- Database: `defaultCatalog`
- Schema: `DEFAULT`
You should be able to see the tables/cubes in the Apache Kylin's project
![Data source](/images/blog/kylin-tableau/kylin_jdbc_tableau_working.jpg)
__Important__: Decide if you want the data source be in `live` or `extract` mode. Some of the functions might not work in `live` mode as for the other data sources - it's just how Tableau works. Recommendation is to start with `live` mode to utilize performance of Apache Kylin. If you're forced to switch to `extract` mode - consider creating a custom query against Apache Kylin's cubes to retrieve as small amount of data as possible as it will help the report to perform well.
5. Finish designing your data source and then switch to worksheets, dashboards

![Tableau Desktop](/images/blog/kylin-tableau/kylin_jdbc_tableau_working_sheet.jpg)

## Publishing reports from Tableau Desktop to Tableau Server

To publish the data source and the report follow these steps:
1. In Tableau Desktop from top menu select Server -> Publish
2. Choose the settings for publishing like Project, select sheets
3. __Important__: For data source Authentication set `Embedded` - this is very important for data refresh to work, however keep in mind that the credentials will be embeded in the report then
4. Publish the report
5. Pop up should be displayed with the preview of the report rendered by the server 

![Tableau Server](/images/blog/kylin-tableau/kylin_jdbc_tableau_server.jpg)

Verify if the report is displaying properly and can connect to Apache Kylin correctly by opening it directly in Tableau Server web application.