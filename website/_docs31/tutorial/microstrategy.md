---
layout: docs31
title:  MicroStrategy
categories: tutorial
permalink: /docs31/tutorial/microstrategy.html
---

### Install ODBC Driver

Refer to this guide: [Kylin ODBC Driver Tutorial](./odbc.html).
Please make sure to download and install Kylin ODBC Driver __v1.6__ 64 bit or above. If you already installed ODBC Driver in your system, please uninstall it first. already installed ODBC Driver in your system, please uninstall it first.  

The Kylin ODBC driver needs to be installed in the machine or virtual environment where your Microstrategy Intelligenec Server is installed. 

###Create Local DSN

Open your window ODBC Data Source Administrator (64bit) and create a system DSN that point to your kylin instance. 

![](/images/tutorial/2.1/MicroStrategy/0.png)

### Setting Database Instance

Connect Kylin using ODBC driver: open your MicroStrategy Developer and connect to the project source where your are going to connect Kylin data source using a user account with administrative privilege. 

Once logged in, go to `Administration` -> `Configuration manager` -> `Database Instance`, create a new database instance with system DSN that you created in the previous step. Under database connection type, please choose Generic DBMS.

![](/images/tutorial/2.1/MicroStrategy/2.png)

![](/images/tutorial/2.1/MicroStrategy/1.png)

Depending on your business scenario, you may need to create a new project and set Kylin database instance as your primary database instance or if there is an existing project, set Kylin database instance as one of your primary or non-primary database instance. You can achieve this by right click on your project, and go to `project configuration` -> `database instance`. 

### Import Logical Table

Open up your project, go to `schema` -> `warehouse catalog` to import the tables your need. 

![](/images/tutorial/2.1/MicroStrategy/4.png)

### Building Schema and Public Objects

Create Attribute, Facts and Metric objects

![](/images/tutorial/2.1/MicroStrategy/5.png)

![](/images/tutorial/2.1/MicroStrategy/6.png)

![](/images/tutorial/2.1/MicroStrategy/7.png)

![](/images/tutorial/2.1/MicroStrategy/8.png)

### Create a Simple Report

Now you can start creating reports with Kylin as data source.

![](/images/tutorial/2.1/MicroStrategy/9.png)

![](/images/tutorial/2.1/MicroStrategy/10.png)

### Best Practice for Connecting MicroStrategy to Kylin Data Source

1. Kylin does not work with multiple SQL passes at the moment, so it is recommended to set up your report intermediate table type as derived, you can change this setting at report level using `Data`-> `VLDB property`-> `Tables`-> `Intermediate Table Type`

2. Avoid using below functionality in MicroStrategy as it will generate multiple sql passes that can not be bypassed by VLDB property:

   ​	Creation of datamarts

   ​	Query partitioned tables

   ​	Reports with custom groups

3. Dimension named with Kylin keywords will cause sql to error out. You may find Kylin keywords here, it is recommended to avoid naming the column name as Kylin keywords, especially when you use MicroStrategy as the front-end BI tool, as far as we know there is no setting in MicroStrategy that can escape the keyword.  [https://calcite.apache.org/docs/reference.html#keywords](https://calcite.apache.org/docs/reference.html#keywords)

4. If underlying Kylin data model has left join from fact table to lookup table, In order for Microstrategy to also generate the same left join in sql, please follow below MicroStrategy TN to modify VLDB property:

   [https://community.microstrategy.com/s/article/ka1440000009GrQAAU/KB17514-Using-the-Preserve-all-final-pass-result-elements-VLDB](https://community.microstrategy.com/s/article/ka1440000009GrQAAU/KB17514-Using-the-Preserve-all-final-pass-result-elements-VLDB)

5. By default, MicroStrategy generate SQL query with date filter in a format like 'mm/dd/yyyy'. This format might be different from Kylin's date format, if so, query will error out. You may follow below steps to change MicroStrategy to generate the same date format SQL as Kylin,  

   1. go to `Instance` -> `Administration` -> `Configuration Manager` -> `Database Instance`. 
   2. Then right click on the database, choose VLDB properties. 
   3. On the top menu choose `Tools` -> `show Advanced Settings`.
   4. Go to `select/insert` -> `date format`.
   5. Change the date format to follow date format in Kylin, for example 'yyyy-mm-dd'.
   6. Restart MicroStrategy Intelligence Server so that change can be effective. 
