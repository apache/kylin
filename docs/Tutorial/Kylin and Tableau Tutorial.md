Kylin and Tableau Tutorial
===

> There are some limitations of Kylin ODBC driver with Tableau, please read carefully this instruction before you try it.
> * Only support "managed" analysis path, Kylin engine will raise exception for un-excepted dimension or metric
> * Please always select Fact Table first, then add lookup tables with correct join condition (defined join type in cube)
> * Do not try to join between fact tables or lookup tables;
> * You can try to use high cardinality dimensions like seller id as Tableau Filter, but the engine will only return limited seller id in Tableau's filter now.

> More detail information or any issue, please contact Kylin Team: `kylinolap@gmail.com`

### Step 1. Install ODBC Driver
Refer to wiki page [Kylin ODBC Driver Tutorial](Kylin ODBC Driver Tutorial.md).

### Step 2. Connect to Kylin Server
> We recommended to use Connect Using Driver instead of Using DSN since Tableau team will not manage your DSN on Tableau Servers.

**The snapshots are a little bit outdated because at that time Kylin ues port 9080 as default port, for new comers please use the new default port 7070**

Connect Using Driver: Select "Other Database(ODBC)" in the left panel and choose KylinODBCDriver in the pop-up window. 

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/1%20odbc.png)

Enter your Sever location and credentials: server host, port, username and password.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/2%20serverhost.jpg)

Click "Connect" to get the list of projects that you have permission to access. See details about permission in [Kylin Cube Permission Grant Tutorial](https://github.com/KylinOLAP/Kylin/wiki/Kylin-Cube-Permission-Grant-Tutorial). Then choose the project you want to connect in the drop down list. 

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/3%20project.jpg)

Click "Done" to complete the connection.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/4%20done.jpg)

### Step 3. Using Single Table or Multiple Tables
> Limitation
>    * Must select FACT table first
>    * Do not support select from lookup table only
>    * The join condition must match within cube definition

**Select Fact Table**

Select `Multiple Tables`.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/5%20multipleTable.jpg)

Then click `Add Table...` to add a fact table.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/6%20facttable.jpg)

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/6%20facttable2.jpg)

**Select Look-up Table**

Click `Add Table...` to add a look-up table. 

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/7%20lkptable.jpg)

Set up the join clause carefully. 

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/8%20join.jpg)

Keep add tables through click `Add Table...` until all the look-up tables have been added properly. Give the connection a name for use in Tableau.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/9%20connName.jpg)

**Using Connect Live**

There are three types of `Data Connection`. Choose the `Connect Live` option. 

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/10%20connectLive.jpg)

Then you can enjoy analyzing with Tableau.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/11%20analysis.jpg)

**Add additional look-up Tables**

Click `Data` in the top menu bar, select `Edit Tables...` to update the look-up table information.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/12%20edit%20tables.jpg)

### Step 4. Using Customized SQL
To use customized SQL resembles using Single Table/Multiple Tables, except that you just need to paste your SQL in `Custom SQL` tab and take the same instruction as above.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/19%20custom.jpg)

### Step 5. Publish to Tableau Server
Suppose you have finished making a dashboard with Tableau, you can publish it to Tableau Server.
Click `Server` in the top menu bar, select `Publish Workbook...`. 

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/14%20publish.jpg)

Then sign in your Tableau Server and prepare to publish. 

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/16%20prepare-publish.png)

If you're Using Driver Connect instead of DSN connect, you'll need to additionally embed your password in. Click the `Authentication` button at left bottom and select `Embedded Password`. Click `Publish` and you will see the result.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/17%20embedded-pwd.png)

### Tips
* Hide Table name in Tableau

    * Tableau will display columns be grouped by source table name, but user may want to organize columns with different structure. Using "Group by Folder" in Tableau and Create Folders to group different columns.

     ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tableau_tutorial/18%20groupby-folder.jpg)