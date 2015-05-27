---
layout: post
title:  "Kylin Cube Permission Grant Tutorial"
date:   2014-11-11
author: Kejia-Wang
categories: tutorial
---

In `Cubes` page, double click the cube row to see the detail information. Here we focus on the `Access` tab.
Click the `+Grant` button to grant permission. 

![](/images/Kylin-Cube-Permission-Grant-Tutorial/14 +grant.png)

There are four different kinds of permissions for a cube. Move your mouse over the `?` icon to see detail information. 

![](/images/Kylin-Cube-Permission-Grant-Tutorial/15 grantInfo.png)

There are also two types of user that a permission can be granted: `User` and `Role`. `Role` means a group of users who have the same role.

### 1. Grant User Permission
* Select `User` type, enter the username of the user you want to grant and select the related permission. 

     ![](/images/Kylin-Cube-Permission-Grant-Tutorial/16 grant-user.png)

* Then click the `Grant` button to send a request. After the success of this operation, you will see a new table entry show in the table. You can select various permission of access to change the permission of a user. To delete a user with permission, just click the `Revoke` button.

     ![](/images/Kylin-Cube-Permission-Grant-Tutorial/16 user-update.png)

### 2. Grant Role Permission
* Select `Role` type, choose a group of users that you want to grant by click the drop down button and select a permission.

* Then click the `Grant` button to send a request. After the success of this operation, you will see a new table entry show in the table. You can select various permission of access to change the permission of a group. To delete a group with permission, just click the `Revoke` button.
