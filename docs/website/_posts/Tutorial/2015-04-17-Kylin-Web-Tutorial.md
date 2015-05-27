---
layout: post
title:  "Kylin Web Tutorial"
date:   2015-04-17
author: Luwei
categories: tutorial
---

> **Supported Browsers**
> 
> Windows: Google Chrome, FireFox
> 
> Mac: Google Chrome, FireFox, Safari

## 1. Access & Login
Host to access: http://your_sandbox_ip:9080
Login with username/password: ADMIN/KYLIN

![](/images/Kylin-Web-Tutorial/1 login.png)

## 2. Available Hive Tables in Kylin
Although Kylin will using SQL as query interface and leverage Hive metadata, kylin will not enable user to query all hive tables since it's a pre-build OLAP (MOLAP) system so far. To enable Table in Kylin, it will be easy to using "Sync" function to sync up tables from Hive.

![](/images/Kylin-Web-Tutorial/2 tables.png)

## 3. Kylin OLAP Cube
Kylin's OLAP Cubes are pre-calculation datasets from Star Schema Hive tables, Here's the web management interface for user to explorer, manage all cubes.Go to `Cubes` Menu, it will list all cubes available in system:

![](/images/Kylin-Web-Tutorial/3 cubes.png)

To explore more detail about the Cube

* Form View:

   ![](/images/Kylin-Web-Tutorial/4 form-view.png)

* SQL View (Hive Query to read data to generate the cube):

   ![](/images/Kylin-Web-Tutorial/5 sql-view.png)

* Visualization (Showing the Star Schema behind of this cube):

   ![](/images/Kylin-Web-Tutorial/6 visualization.png)

* Access (Grant user/role privileges, Grant operation only open to Admin in beta):

   ![](/images/Kylin-Web-Tutorial/7 access.png)

## 4. Write and Execute SQL on web
Kylin's web offer a simple query tool for user to run SQL to explorer existing cube, verify result and explorer the result set using #5's Pivot analysis and visualization

> **Query Limit**
> 
> 1. Only SELECT query be supported
> 
> 2. To avoid huge network traffic from server to client, the scan range's threshold be set to 1,000,000 in beta.
> 
> 3. SQL can't found data from cube will not redirect to Hive in beta

Go to "Query" menu:

![](/images/Kylin-Web-Tutorial/8 query.png)

* Source Tables:

   Browser current available Tables (same structure and metadata as Hive):
  
   ![](/images/Kylin-Web-Tutorial/9 query-table.png)

* New Query:

   You can write and execute your query and explorer the result. One query for you reference:

   ![](/images/Kylin-Web-Tutorial/10 query-result.png)

* Saved Query:

   Associate with user account, you can get saved query from different browsers even machines.
   Click "Save" in Result area, it will popup for name and description to save current query:

   ![](/images/Kylin-Web-Tutorial/11 save-query.png)

   Click "Saved Queries" to browser all your saved queries, you could direct resubmit it to run or remove it:

   ![](/images/Kylin-Web-Tutorial/11 save-query-2.png)

* Query History:

   Only keep the current user's query history in current bowser, it will require cookie enabled and will lost if you clean up bowser's cache.Click "Query History" tab, you could directly resubmit any of them to execute again.

## 5. Pivot Analysis and Visualization
There's one simple pivot and visualization analysis tool in Kylin's web for user to explore their query result:

* General Information:

   When the query execute success, it will present a success indictor and also a cube's name which be hit. 
   Also it will present how long this query be executed in backend engine (not cover network traffic from Kylin server to browser):

   ![](/images/Kylin-Web-Tutorial/12 general.png)

* Query Result:

   It's easy to order on one column.

   ![](/images/Kylin-Web-Tutorial/13 results.png)

* Export to CSV File

   Click "Export" button to save current result as CSV file.

* Pivot Table:

   Drag and Drop one or more columns into the header, the result will grouping by such column's value:

   ![](/images/Kylin-Web-Tutorial/14 drag.png)

* Visualization:

   Also, the result set will be easy to show with different charts in "Visualization":

   note: line chart only available when there's at least one dimension with real "Date" data type of column from Hive Table.

   * Bar Chart:

   ![](/images/Kylin-Web-Tutorial/15 bar-chart.png)
   
   * Pie Chart:

   ![](/images/Kylin-Web-Tutorial/16 pie-chart.png)

   * Line Chart

   ![](/images/Kylin-Web-Tutorial/17 line-chart.png)

## 6. Cube Build Job Monitoring
Monitor and manage cube build process, diagnostic into the detail and even link to Hadoop's job information directly:

![](/images/Kylin-Web-Tutorial/7 job-steps.png)
