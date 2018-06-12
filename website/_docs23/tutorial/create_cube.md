---
layout: docs23
title:  Cube Wizard
categories: tutorial
permalink: /docs23/tutorial/create_cube.html
---

This tutorial will guide you to create a cube. It need you have at least 1 sample table in Hive. If you don't have, you can follow this to create some data.
  
### I. Create a Project
1. Go to `Model` page in top menu bar, then click `Manage Projects`.

   ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/1 manage-prject.png)

2. Click the `+ Project` button to add a new project or ignore the first step and click `Add Project`(button on the right) of the first picture.

   ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/2 +project.png)

3. Enter a project name, e.g, "Tutorial", with a description (optional) and the config (optional), then click `submit` button to send the request.

   ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/3 new-project.png)

4. After success, the project will show in the table.

   ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/3.1 pj-created.png)

### II. Sync up Hive Table
1. Click `Model` in top bar and then click `Data Source` tab in the left part, it lists all the tables loaded into Kylin; click `Load Table` button.

   ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/4 +table.png)

2. Enter the hive table names, separated with commad, and then click `Sync` to send the request.

   ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/5 hive-table.png)

3. [Optional] If you want to browser the hive database to pick tables, click the `Load Table From Tree` button.

   ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/4 +table-tree.png)

4. [Optional] Expand the database node, click to select the table to load, and then click `Sync`.

   ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/5 hive-table-tree.png)

5. A success message will pop up. In the left `Tables` section, the newly loaded table is added. Click the table name will expand the columns.

   ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/5 hive-table-info.png)

6. In the background, Kylin will run a MapReduce job to calculate the approximate cardinality for the newly synced table. After the job be finished, refresh web page and then click the table name, the cardinality will be shown in the table info.

   ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/5 hive-table-cardinality.png)


### III. Create Data Model
Before create a cube, need define a data model. The data model defines the star schema. One data model can be reused in multiple cubes.

1. Click `Model` in top bar, and then click `Models` tab. Click `+New` button, in the drop-down list select `New Model`.

    ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/6 +model.png)

2. Enter a name for the model, with an optional description.

    ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/6 model-name.png)

3. In the `Fact Table` box, select the fact table of this data model.

    ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/6 model-fact-table.png)

4. [Optional] Click `Add Lookup Table` button to add a lookup table. Select the table name and join type (inner join or left join).

    ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/6 model-lookup-table.png)

5. [Optional] Click `New Join Condition` button, select the FK column of fact table in the left, and select the PK column of lookup table in the right side. Repeat this if have more than one join columns.

    ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/6 model-join-condition.png)

6. Click "OK", repeat step 4 and 5 to add more lookup tables if any. After finished, click "Next".

7. The "Dimensions" page allows to select the columns that will be used as dimension in the child cubes. Click the `Columns` cell of a table, in the drop-down list select the column to the list. 

    ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/6 model-dimensions.png)

8. Click "Next" go to the "Measures" page, select the columns that will be used in measure/metrics. The measure column can only from fact table. 

    ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/6 model-measures.png)

9. Click "Next" to the "Settings" page. If the data in fact table increases by day, select the corresponding date column in the `Partition Date Column`, and select the date format, otherwise leave it as blank.

10. [Optional] Choose whether has a separate "time of the day" column, by default it is `No`. If choose `Yes`, select the corresponding time column in the `Partition Time Column`, and select the time format.

11. [Optional] If some records want to excluded from the cube, like dirty data, you can input the condition in `Filter`.

    ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/6 model-partition-column.png)

12. Click `Save` and then select `Yes` to save the data model. After created, the data model will be shown in the left `Models` list.

    ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/6 model-created.png)

### IV. Create Cube
After the data model be created, you can start to create cube. 

Click `Model` in top bar, and then click `Models` tab. Click `+New` button, in the drop-down list select `New Cube`.

    ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/7 new-cube.png)

**Step 1. Cube Info**

Select the data model, enter the cube name; Click `Next` to enter the next step.

You can use letters, numbers and '_' to name your cube (blank space in name is not allowed). `Notification Email List` is a list of email addresses which be notified on cube job success/failure. `Notification Events` is the status to trigger events.

    ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/7 cube-info.png)
    
**Step 2. Dimensions**

1. Click `Add Dimension`, it pops up a window: tick columns that you need from FactTable and LookupTable. There are two options for LookupTable columns: "Normal" and "Derived" (default)。"Normal" is to add a normal independent dimension column, "Derived" is to add a derived dimension column. Read more in [How to optimize cubes](/docs15/howto/howto_optimize_cubes.html).

   ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/7 cube-dimension-batch.png)

2. Click "Next" after select all dimensions.

**Step 3. Measures**

1. Click the `+Measure` to add a new measure.

   ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/8 meas-+meas.png)

2. There are 8 types of measure according to its expression: `SUM`, `MAX`, `MIN`, `COUNT`, `COUNT_DISTINCT`, `TOP_N`, `EXTENDED_COLUMN` and `PERCENTILE`. Properly select the return type for `COUNT_DISTINCT` and `TOP_N`, as it will impact on the cube size.
   * SUM

     ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/8 measure-sum.png)

   * MIN

     ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/8 measure-min.png)

   * MAX

     ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/8 measure-max.png)

   * COUNT

     ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/8 measure-count.png)

   * DISTINCT_COUNT
   This measure has two implementations: 
   a) approximate implementation with HyperLogLog, select an acceptable error rate, lower error rate will take more storage.
   b) precise implementation with bitmap (see limitation in https://issues.apache.org/jira/browse/KYLIN-1186). 

     ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/8 measure-distinct.png)

   Pleaste note: distinct count is a very heavy data type, it is slower to build and query comparing to other measures.

   * TOP_N
   Approximate TopN measure pre-calculates the top records in each dimension combination, it will provide higher performance in query time than no pre-calculation; Need specify two parameters here: the first is the column will be used as metrics for Top records (aggregated with SUM and then sorted in descending order); the second is the literal ID, represents the record like seller_id;

   Properly select the return type, depends on how many top records to inspect: top 10, top 100, top 500, top 1000, top 5000 or top 10000. 

     ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/8 measure-topn.png)

* EXTENDED_COLUMN
   Extended_Column as a measure rather than a dimension is to save space. One column with another column can generate new columns.

     ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/8 measure-extended_column.png)

* PERCENTILE
   Percentile represent the percentage. The larger the value, the smaller the error. 100 is the most suitable.

     ![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/8 measure-percentile.png)

**Step 4. Refresh Setting**

This step is designed for incremental cube build. 

`Auto Merge Thresholds`: merge the small segments into medium and large segment automatically. If you don't want to auto merge, remove the default two ranges.

`Volatile Range`: by default it is 0, which will auto merge all possible cube segments, or 'Auto Merge' will not merge latest [Volatile Range] days cube segments.

`Retention Threshold`: only keep the segment whose data is in past given days in cube, the old segment will be automatically dropped from head; 0 means not enable this feature.

`Partition Start Date`: the start date of this cube.

![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/9 refresh-setting1.png)

**Step 5. Advanced Setting**

`Aggregation Groups`: by default Kylin put all dimensions into one aggregation group; you can create multiple aggregation groups by knowing well about your query patterns. For the concepts of "Mandatory Dimensions", "Hierarchy Dimensions" and "Joint Dimensions", read this blog: [New Aggregation Group](/blog/2016/02/18/new-aggregation-group/)

`Rowkeys`: the rowkeys are composed by the dimension encoded values. "Dictionary" is the default encoding method; If a dimension is not fit with dictionary (e.g., cardinality > 10 million), select "false" and then enter the fixed length for that dimension, usually that is the max length of that column; if a value is longer than that size it will be truncated. Please note, without dictionary encoding, the cube size might be much bigger.

You can drag & drop a dimension column to adjust its position in rowkey; Put the mandantory dimension at the begining, then followed the dimensions that heavily involved in filters (where condition). Put high cardinality dimensions ahead of low cardinality dimensions.

`Mandatory Cuboids`: ensure the cuboid that you want to build can build smoothly.

`Cube Engine`: the engine for building cube. There are 2 types: MapReduce and Spark.

`Advanced Dictionaries`: "Global Dictionary" is the default dict for precise count distinct measure, which support rollup among all segments.

"Segment Dictionary" is the special dict for precise count distinct measure, which is based on one segment and could not support rollup among segments. Specifically, if your cube isn't partitioned or you can ensure all your SQLs will group by your partition_column, you could use "Segment Dictionary" instead of "Global Dictionary".

`Advanced Snapshot Table`: design for global lookup table and provide different storage type.

`Advanced ColumnFamily`: If there are more than one ultrahigh cardinality precise count distinct measures, you could assign these measures to more column family.

**Step 6. Configuration Overwrites**

Cube level properties will overwrite configuration in kylin.properties, if you don't have anything to config, click `Next` button.

![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/10 configuration.png)

**Step 7. Overview & Save**

You can overview your cube and go back to previous step to modify it. Click the `Save` button to complete the cube creation.

![]( /images/tutorial/1.5/Kylin-Cube-Creation-Tutorial/11 overview.png)

Cheers! now the cube is created, you can go ahead to build and play it.
