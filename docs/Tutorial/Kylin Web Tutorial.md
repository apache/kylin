Kylin Web Tutorial
===

> **Supported Browsers**

> Windows: Google Chrome, FireFox

> Mac: Google Chrome, FireFox, Safari

## 1. Access & Login
Host to access: http://your_sandbox_ip:7070/kylin
Login with username/password: ADMIN/KYLIN

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/1%20login.png)

## 2. Available Hive Tables in Kylin
Although Kylin will using SQL as query interface and leverage Hive metadata, kylin will not enable user to query all hive tables since it's a pre-build OLAP (MOLAP) system so far. To enable Table in Kylin, it will be easy to using "Sync" function to sync up tables from Hive.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/2%20tables.png)

## 3. Kylin OLAP Cube

> To make cubes availabe you'll have to create them first [Kylin Cube Creation Tutorial](Kylin Cube Creation Tutorial.md)

Kylin's OLAP Cubes are pre-calculation datasets from Star Schema Hive tables, Here's the web management interface for user to explorer, manage all cubes.Go to `Cubes` Menu, it will list all cubes available in system:

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/3%20cubes.png)

To explore more detail about the Cube

* Form View:

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/4%20form-view.png)

* SQL View (Underline Hive Query to generate the cube):

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/5%20sql-view.png)

* Visualization (Showing the Star Schema behind of this cube):

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/6%20visualization.png)

* Access (Grant user/role privileges, Grant operation only open to Admin in beta):

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/7%20access.png)

## 4. Write and Execute SQL on web

>> To make queries availabe you'll have to create are build cubes first [Kylin Cube Creation Tutorial](Kylin Cube Creation Tutorial.md) and [Kylin Cube Build and Job Monitoring Tutorial](Kylin Cube Build and Job Monitoring Tutorial.md)

Kylin's web offer a simple query tool for user to run SQL to explorer existing cube, verify result and explorer the result set using #5's Pivot analysis and visualization

> **Query Limit**

> 1. Only SELECT query be supported

> 2. To avoid huge network traffic from server to client, the underline scan range's threshold be set to 1,000,000 in beta.

> 3. SQL can't found data from cube will not redirect to Hive in beta

Go to "Query" menu:

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/8%20query.png)

* Source Tables:

   Browser current available Tables (same structure and metadata as Hive):
  
   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/9%20query-table.png)

* New Query:

   You can write and execute your query and explorer the result. One query for you experience:

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/10%20query-result.png)

* Saved Query:

   Associate with user account, you can get saved query from different browsers even machines.
   Click "Save" in Result area, it will popup for name and description to save current query:

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/11%20save-query.png)

   Click "Saved Queries" to browser all your saved queries, you could direct resubmit it to run or remove it:

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/11%20save-query-2.png)

* Query History:

   Only keep the current user's query history in current bowser, it will require cookie enabled and will lost if you clean up bowser's cache.Click "Query History" tab, you could directly resubmit any of them to execute again.

## 5. Pivot Analysis and Visualization
There's one simple pivot and visualization analysis tool in Kylin's web for user to explorer their query result:

* General Information:

   When the query execute success, it will present a success indictor and also a cube's name which be hit. 
   Also it will present how long this query be executed in backend engine (not cover network traffic from Kylin server to browser):

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/12%20general.png)

* Query Result:

   It's easy to order on one column.

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/13%20results.png)

* Export to CSV File

   Click "Export" button to save current result as CSV file.

* Pivot Table:

   Drag and Drop one or more columns into the header, the result will grouping by such column's value:

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/14%20drag.png)

* Visualization:

   Also, the result set will be easy to show with different charts in "Visualization":

   note: line chart only available when there's at least one dimension with real "Date" data type of column from Hive Table.

   * Bar Chart:

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/15%20bar-chart.png)
   
   * Pie Chart:

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/16%20pie-chart.png)

   * Line Chart

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/web%20tutorial/17%20line-chart.png)

## 6. Cube Build Job Monitoring
Monitor and manage cube build process, diagnostic into the detail and even link to Hadoop's job information directly:

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/cube_build_job_monitor/7%20job-steps.png)