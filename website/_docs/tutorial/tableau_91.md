---
layout: docs
title:  Tableau 9 Tutorial
categories: tutorial
permalink: /docs/tutorial/tableau_91.html
version: v1.2
since: v1.2
---

Tableau 9.x has been released a while, there are many users are asking about support this version with Apache Kylin. With updated Kylin ODBC Driver, now user could interactive with Kylin service through Tableau 9.x.

> Apache Kylin currently doesn't support query on raw data yet, some queries might fail and cause some exceptions in application. Patch [KYLIN-1075](https://issues.apache.org/jira/browse/KYLIN-1075) is recommended to get better look of query result.

### For Tableau 8.x User
Please refer to [Kylin and Tableau Tutorial](./tableau.html) for detail guide.

### Install Kylin ODBC Driver
Refer to this guide: [Kylin ODBC Driver Tutorial](./odbc.html).
Please make sure to download and install Kylin ODBC Driver __v1.2__. If you already installed ODBC Driver in your system, please uninstall it first. 

### Connect to Kylin Server
Connect Using Driver: Start Tableau 9.1 desktop, click `Other Database(ODBC)` in the left panel and choose KylinODBCDriver in the pop-up window. 
![](/images/tutorial/odbc/tableau_91/1.png)

Provide your Sever location, credentials and project. Clicking `Connect` button, you can get the list of projects that you have permission to access, see details at [Kylin Cube Permission Grant Tutorial](./acl.html).
![](/images/tutorial/odbc/tableau_91/2.png)

### Mapping Data Model
In left panel, select `defaultCatalog` as Database, click `Search` button in Table search box, and all tables get listed. With drag and drop to the right region, tables will become data source. Make sure JOINs are configured correctly.
![](/images/tutorial/odbc/tableau_91/3.png)

### Connect Live
There are two types of `Connection`, choose the `Live` option to make sure using Connect Live mode.
![](/images/tutorial/odbc/tableau_91/4.png)

### Custom SQL
To use customized SQL, click `New Custom SQL` in left panel and type SQL statement in pop-up dialog.
![](/images/tutorial/odbc/tableau_91/5.png)

### Visualization
Now you can start to enjou analyzing with Tableau 9.1.
![](/images/tutorial/odbc/tableau_91/6.png)

### Publish to Tableau Server
If you want to publish local dashboard to a Tableau Server, just expand `Server` menu and select `Publish Workbook`.
![](/images/tutorial/odbc/tableau_91/7.png)

### More
Please refer to [Kylin and Tableau Tutorial](./tableau.html) for more detail.


