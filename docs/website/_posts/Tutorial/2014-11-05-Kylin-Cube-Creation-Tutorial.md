---
layout: post
title:  "Kylin Cube Creation Tutorial"
date:   2014-11-15
author: Kejia-Wang
categories: tutorial
---

### I. Create a Project
1. Go to `Query` page in top menu bar, then click `Manage Projects`.

   ![]( /images/Kylin-Cube-Creation-Tutorial/1 manage-prject.png)

2. Click the `+ Project` button to add a new project.

   ![]( /images/Kylin-Cube-Creation-Tutorial/2 +project.png)

3. Fulfill the following form and click `submit` button to send a request.

   ![]( /images/Kylin-Cube-Creation-Tutorial/3 new-project.png)

4. After success, there will be a notification show in the bottom.

   ![]( /images/Kylin-Cube-Creation-Tutorial/3.1 pj-created.png)

### II. Sync up a Table
1. Click `Tables` in top bar and then click the `+ Sync` button to load hive table metadata.

   ![]( /images/Kylin-Cube-Creation-Tutorial/4 +table.png)

2. Enter the table names and click `Sync` to send a request.

   ![]( /images/Kylin-Cube-Creation-Tutorial/5 hive-table.png)

### III. Create a Cube
To start with, click `Cubes` in top bar.Then click `+Cube` button to enter the cube designer page.

![]( /images/Kylin-Cube-Creation-Tutorial/6 +cube.png)

**Step 1. Cube Info**

Fill up the basic information of the cube. Click `Next` to enter the next step.

You can use letters, numbers and '_' to name your cube (Notice that space in name is not allowed).

![]( /images/Kylin-Cube-Creation-Tutorial/7 cube-info.png)

**Step 2. Dimensions**

1. Set up the fact table.

    ![]( /images/Kylin-Cube-Creation-Tutorial/8 dim-factable.png)

2. Click `+Dimension` to add a new dimension.

    ![]( /images/Kylin-Cube-Creation-Tutorial/8 dim-+dim.png)

3. There are different types of dimensions that might be added to a cube. Here we list some of them for your reference.

    * Dimensions from fact table.
        ![]( /images/Kylin-Cube-Creation-Tutorial/8 dim-typeA.png)

    * Dimensions from look up table.
        ![]( /images/Kylin-Cube-Creation-Tutorial/8 dim-typeB-1.png)

        ![]( /images/Kylin-Cube-Creation-Tutorial/8 dim-typeB-2.png)
   
    * Dimensions from look up table with hierarchy.
        ![]( /images/Kylin-Cube-Creation-Tutorial/8 dim-typeC.png)

    * Dimensions from look up table with derived dimensions.
        ![]( /images/Kylin-Cube-Creation-Tutorial/8 dim-typeD.png)

4. User can edit the dimension after saving it.
   ![]( /images/Kylin-Cube-Creation-Tutorial/8 dim-edit.png)

**Step 3. Measures**

1. Click the `+Measure` to add a new measure.
   ![]( /images/Kylin-Cube-Creation-Tutorial/9 meas-+meas.png)

2. There are 5 different types of measure according to its expression: `SUM`, `MAX`, `MIN`, `COUNT` and `COUNT_DISTINCT`. Please be  carefully to choose the return type, which is related to the error rate of the `COUNT(DISTINCT)`.
   * SUM

     ![]( /images/Kylin-Cube-Creation-Tutorial/9 meas-sum.png)

   * MIN

     ![]( /images/Kylin-Cube-Creation-Tutorial/9 meas-min.png)

   * MAX

     ![]( /images/Kylin-Cube-Creation-Tutorial/9 meas-max.png)

   * COUNT

     ![]( /images/Kylin-Cube-Creation-Tutorial/9 meas-count.png)

   * DISTINCT_COUNT

     ![]( /images/Kylin-Cube-Creation-Tutorial/9 meas-distinct.png)

**Step 4. Filter**

This step is optional. You can add some condition filter in `SQL` format.

![]( /images/Kylin-Cube-Creation-Tutorial/10 filter.png)

**Step 5. Refresh Setting**

This step is designed for incremental cube build. 

![]( /images/Kylin-Cube-Creation-Tutorial/11 refresh-setting1.png)

Choose partition type, partition column and start date.

![]( /images/Kylin-Cube-Creation-Tutorial/11 refresh-setting2.png)

**Step 6. Advanced Setting**

![]( /images/Kylin-Cube-Creation-Tutorial/12 advanced.png)

**Step 7. Overview & Save**

You can overview your cube and go back to previous step to modify it. Click the `Save` button to complete the cube creation.

![]( /images/Kylin-Cube-Creation-Tutorial/13 overview.png)
