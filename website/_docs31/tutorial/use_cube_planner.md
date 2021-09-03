---
layout: docs31
title:  Use Cube Planner
categories: tutorial
permalink: /docs31/tutorial/use_cube_planner.html
---

> Available since Apache Kylin v2.3.0

# Cube Planner

## What is Cube Planner

OLAP solution trades off online query speed with offline Cube build cost (compute resource to build Cube and storage resource to save the Cube data). Resource efficiency is the most important competency of OLAP engine. To be resource efficient, It is critical to just pre-build the most valuable cuboids.

Cube Planner makes Apache Kylin to be more resource efficient. It intelligently build a partial Cube to minimize the cost of building a Cube and at the same time maximize the benefit of serving end user queries, then learn patterns from queries at runtime and dynamically recommend cuboids accordingly. 

![CubePlanner](/images/CubePlanner/CubePlanner.png)

Read more at [eBay tech blog](https://www.ebayinc.com/stories/blogs/tech/cube-planner-build-an-apache-kylin-olap-cube-efficiently-and-intelligently/)

## Prerequisites

To enable Dashboard on WebUI, you need to set `kylin.cube.cubeplanner.enabled=true` and other properties in `kylin.properties`

{% highlight Groff markup %}
kylin.cube.cubeplanner.enabled=true
kylin.server.query-metrics2-enabled=true
kylin.metrics.reporter-query-enabled=true
kylin.metrics.reporter-job-enabled=true
kylin.metrics.monitor-enabled=true
{% endhighlight %}

## How to use it

*Note: Cube planner is divided into two phase. Phase 1 can recommend cuboid list based on estimated cuboid size before building the Cube, while phase 2 recommends cuboid list for existing Cube according to query statistics. Cube should be online on production for a while (like 3 months) before optimizing it. So that Kylin platform collects enough real queries from end user and use them to optimize the Cube.*  

#### Step 1:

​	Select a Cube

#### Step 2:

1. Click the '**Planner**' button to view the '**Current Cuboid Distribution**' of the Cube.

  You should make sure the status of the Cube is '**READY**'

  If the status of the Cube is '**DISABLED**', you will not be able to use the Cube planner.

  You should change the status of the Cube from '**DISABLED**' to '**READY**' by building it or enabling it if it has been built before.


#### Step 3:

a. Click the '**Planner**' button to view the '**Current Cuboid Distribution**' of the Cube.

- The data will be displayed in **Sunburst Chart**. 

- Each part refers to a cuboid, is shown in different colors determined by the query **frequency** against this cuboid.

     ![CubePlanner](/images/CubePlanner/CP.png)


-  You can move the cursor over the chart and it will display the detail information of the cuboid.

   The detail information contains 5 attributes, '**Name**', '**ID**', '**Query Count**', '**Exactly Match Count**', '**Row Count**' and '**Rollup Rate**'. 

   Cuboid **Name** is composed of several '0' or '1'. It means a combination of dimensions. '0' means the dimension doesn't exist in this combination, while '1' means the dimension exist in the combination. All the dimensions are ordered by the HBase row keys in advanced settings. 

   Here is an example: 

   ![CubePlanner](/images/CubePlanner/Leaf.png)

   Name "1111111110000000" means the dimension combination is ["MONTH_BEG_DT","USER_CNTRY_SITE_CD","RPRTD_SGMNT_VAL","RPRTD_IND","SRVY_TYPE_ID","QSTN_ID","L1_L2_IND","PRNT_L1_ID","TRANCHE_ID"] based on the row key orders.

   **ID** is the unique id of the cuboid.

   **Query Count** is the total count of the queries that are served by this cuboid, including those queries that against other un-precalculated cuboids, but on line aggregated from this cuboid.  

   **Exactly Match Count** is the query count that the query is actually against this cuboid.

   **Row Count** is the total row count of all the segments for this cuboid.

   **Rollup Rate** = (Cuboid's Row Count/its parent cuboid's Row Count) * 100%  

-  The center of the sunburst chart contains the combined information of  basic cuboid. its '**Name**' is composed of several '1's.

As for a leaf, its '**Name**' is composed of several '0's and 1's. 

-    If you want to specify a leaf, just click on it. The view will change automatically.

     ![Leaf-Specify](/images/CubePlanner/Leaf-Specify.png)

-    If you want to specify the parent leaf of a leaf, click on the **center circle** (the part marked yellow).

![Leaf-Specify-Parent](/images/CubePlanner/Leaf-Specify-Parent.png)

b. Click the '**Recommend**' button to view the '**Recommend Cuboid Distribution**' of the Cube.

If the Cube is currently under building, the Cube planner '**Recommend**' function will not be able to perform. Please wait the build to finish.

-  The data will be calculated by unique algorithms. It is common to see this window.

   ![Recommending](/images/CubePlanner/Recommending.png)

-  The data will be displayed in Sunburst Chart.

   - Each part is shown in different colors determined by the **frequency**.

![CubePlanner_Recomm](/images/CubePlanner/CPRecom.png)

- Detailed operation of the '**Recommend Cuboid Distribution**' chart is the same as '**Current Cuboid Distribution**' chart.
- User is able to tell the dimension names from a cuboid when mouse hovers over the sunburst chart as figure shown below.
- User is able to click **'Export'** to export hot dimension combinations (TopN cuboids, currently including options of Top 10, Top 50, Top 100) from an existing Cube as a json file, which will be downloaded to your local file system for recording or future import of dimension combinations when creating Cube.

![export cuboids](/images/CubePlanner/export_cuboids.png)

c. Click the '**Optimize**' button to optimize the Cube.

- A window will jump up to confirm. Click '**Yes**' to start the optimization. Click '**Cancel**' to give up the optimization.

- User is able to get to know the last optimized time of the Cube in Cube Planner tab page. 

Please note: if you don't see the last optimized time, upgrade to Kylin v2.3.2 or above, check KYLIN-3404.

![column name+optimize time](/images/CubePlanner/column_name+optimize_time.png)

- User is able to receive an email notification for a Cube optimization job.

![optimize email](/images/CubePlanner/optimize_email.png)
