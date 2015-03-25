Kylin Cube Creation Tutorial
===

### I. Create a Project
1. Go to `Query` page in top menu bar, then click `Manage Projects`.

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/1%20manage-prject.png)

2. Click the `+ Project` button to add a new project.

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/2%20%2Bproject.png)

3. Fulfill the following form and click `submit` button to send a request.

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/3%20new-project.png)

4. After success, there will be a notification show in the bottom.

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/3.1%20pj-created.png)

### II. Sync up a Table
1. Click `Tables` in top bar and then click the `+ Sync` button to load hive table metadata.

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/4%20%2Btable.png)

2. Enter the table names and click `Sync` to send a request.

   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/5%20hive-table.png)

### III. Create a Cube
To start with, click `Cubes` in top bar.Then click `+Cube` button to enter the cube designer page.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/6%20%2Bcube.png)

**Step 1. Cube Info**

Fill up the basic information of the cube. Click `Next` to enter the next step.

You can use letters, numbers and '_' to name your cube (Notice that space in name is not allowed).

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/7%20cube-info.png)

**Step 2. Dimensions**

1. Set up the fact table.

    ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/8%20dim-factable.png)

2. Click `+Dimension` to add a new dimension.

    ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/8%20dim-%2Bdim.png)

3. There are different types of dimensions that might be added to a cube. Here we list some of them for your reference.

    * Dimensions from fact table.
          ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/8%20dim-typeA.png)

    * Dimensions from look up table.
          ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/8%20dim-typeB-1.png)

          ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/8%20dim-typeB-2.png)     
   
    * Dimensions from look up table with hierarchy.
          ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/8%20dim-typeC.png)

    * Dimensions from look up table with derived dimensions.
          ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/8%20dim-typeD.png)

4. User can edit the dimension after saving it.
   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/8%20dim-edit.png)

**Step 3. Measures**

1. Click the `+Measure` to add a new measure.
   ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/9%20meas-%2Bmeas.png)

2. There are 5 different types of measure according to its expression: `SUM`, `MAX`, `MIN`, `COUNT` and `COUNT_DISTINCT`. Please be  carefully to choose the return type, which is related to the error rate of the `COUNT(DISTINCT)`.
   * SUM

     ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/9%20meas-sum.png)

   * MIN

     ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/9%20meas-min.png)

   * MAX

     ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/9%20meas-max.png)

   * COUNT

     ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/9%20meas-count.png)

   * DISTINCT_COUNT

     ![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/9%20meas-distinct.png)

**Step 4. Filter**

This step is optional. You can add some condition filter in `SQL` format.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/10%20filter.png)

**Step 5. Refresh Setting**

This step is designed for incremental cube build. 

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/11%20refresh-setting1.png)

Choose partition type, partition column and start date.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/11%20refresh-setting2.png)

**Step 6. Advanced Setting**

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/12%20advanced.png)

**Step 7. Overview & Save**

You can overview your cube and go back to previous step to modify it. Click the `Save` button to complete the cube creation.

![](https://raw.githubusercontent.com/KylinOLAP/kylinolap.github.io/master/docs/tutorial/13%20overview.png)

### IV. What's next

After cube being created, you might need to:

1. [Build the cube so that it can be queried](Kylin Cube Build and Job Monitoring Tutorial.md)
2. [Grant permission to cubes](Kylin Cube Permission Grant Tutorial.md)