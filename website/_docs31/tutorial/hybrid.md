---
layout: docs31
title: Hybrid Model
categories: tutorial
permalink: /docs31/tutorial/hybrid.html
since: v2.5.0
---

This tutorial will guide you to create a hybrid model. Regarding the concept of hybrid, please refer to [this blog](http://kylin.apache.org/blog/2015/09/25/hybrid-model/).

### I. Create a hybrid model
One Hybrid model can refer to multiple cubes.

1. Click `Model` in top bar, and then click `Models` tab. Click `+New` button, in the drop-down list select `New Hybrid`.

    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/1 +hybrid.png)

2. Enter a name for the hybrid, select the data model, and then check the box for the cubes that you want to add, click > button to add the cube to this hybrid.

    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/2 hybrid-name.png)
    
*Note: If you want to change the data model, you need to remove all the cubes that you already selected.* 

3. Click `Submit` to save the Hybrid model. After be created, the hybrid model will be shown in the left `Hybrids` list.
    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/3 hybrid-created.png)

### II. Update a hybrid model
1. Place the mouse over the hybrid name, then click `Action` button, in the drop-down list select `Edit`. You can update the hybrid by adding(> button) or deleting(< button) cubes to/from it. 
    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/4 edit-hybrid.png)

2. Click `Submit` to save the Hybrid model. 

Now you only can view hybrid details by click `Edit` button.

### III. Drop a hybrid model
1. Place the mouse over the Hybrid name, then click `Action` button, in the drop-down list select `Drop`. Then the window will pop up. 

2. Click `Yes` to drop the Hybrid model. 

### IV. Run Query
After the hybrid model is created, you can run a query. As the hybrid has higher priority than the cube, queries will already hit the hybrid model, and then be delegated to cubes. 

Click `Insight` in top bar, input a SQL statement to execute.
    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/5 sql-statement.png)

*Please note, Hybrid model is not suitable for "bitmap" count distinct measures's merge across cubes, please have the partition date as a group by field in the SQL query. *
