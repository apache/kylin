---
layout: docs
title: Hybrid Model
categories: tutorial
permalink: /docs/tutorial/hybrid.html
since: v0.7.1
---

This tutorial will guide you to create a Hybrid. 

### I. Create Hybrid Model
One Hybrid model can be referenced by multiple cubes.

1. Click `Model` in top bar, and then click `Models` tab. Click `+New` button, in the drop-down list select `New Hybrid`.

    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/1 +hybrid.png)

2. Enter a name for the Hybrid, then choose the model including cubes that you want to query, and then check the box before cube name, click > button to add cube(s) to hybrid.

    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/2 hybrid-name.png)
    
*Note: If you want to change the model, you should remove all the cubes that you selected.* 

3. Click `Submit` and then select `Yes` to save the Hybrid model. After created, the Hybrid model will be shown in the left `Hybrids` list.
    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/3 hybrid-created.png)

### II. Update Hybrid Model
1. Place the mouse over the Hybrid name, then click `Action` button, in the drop-down list select `Edit`. Then you can update Hybrid by adding(> button) or deleting(< button) cubes. 
    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/4 edit-hybrid.png)

2. Click `Submit` and then select `Yes` to save the Hybrid model. 

Now you only can view Hybrid details by click `Edit` button.

### III. Drop Hybrid Model
1. Place the mouse over the Hybrid name, then click `Action` button, in the drop-down list select `Drop`. Then the window will pop up. 

2. Click `Yes` to delete the Hybrid model. 

### IV. Run Query
After the Hybrid model is created, you can run a query directly. 

Click `Insight` in top bar, and then input sql statement of you needs.
    ![]( /images/tutorial/2.5/Kylin-Hybrid-Creation-Tutorial/5 sql-statement.png)


Please refer to [this blog](http://kylin.apache.org/blog/2015/09/25/hybrid-model/) for other matters.