---
title: Table Access Control
language: en
sidebar_label: Table Access Control
pagination_label: Table Access Control
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - table access control
draft: true
last_update:
    date: Aug 16, 2022
---

### <span id="table">Table ACL</span>

**Introduction**

Table ACLs determines whether a user/user group can access a certain table loaded into Kylin. When a user/group is restricted to a table, the user/user group cannot query the table. 

- System administrator can grant table-level data access to a user/user group. All users/user groups that have access rights to the current project can read or query all tables in the current project by default.
- When the access rights of the user/user group are deleted, or the user/user group is removed from the system, the corresponding table-level permissions are also deleted.
- When the table is deleted from the project, the Table ACL for all users/user groups are also deleted.
- When the table is reloaded in the project, the Table ACL for all users/user groups are retained.

**Example**

Now we will use specific examples to show the product behavior after setting table ACLs.

If a user do not have the access to table `SSB.SUPPLIER`, he cannot see the corresponding table under the project or in the connected BI tool.

![Set Table ACL](#TODO)

![The table is not visible](#TODO)

If you query the table, the result will show an error.

![Result shows an error](#TODO)

### <span id="set">Grant Table ACL</span>

1. In the **Admin** -> Project page, the system administrator can grant user/user group project-level access rights. For details, please refer to [Project ACL](project_acl.md).

2. Select the project you want to authorize，then click **Authorization** under the right **Action** bar and go to the authorization page.

3. Click the left arrow button and expand the user/user group that requires authorization, and the interface displays the tables / columns / rows that the user/user group has access to.

   ![Grant Table ACL](#TODO)

4. Click the **Edit** button and check the tables and columns that need to be granted access. There is no row-level access restriction by default, which means that users can access all the rows in the column.

   ![Edit Table ACL](#TODO)

5. Click the **+ Add** button on the right of the **Row Access List**, select the column in the pop-up window and enter the rows that can be accessed. Support for **IN** or **LIKE** rules to set row level permissions, increase or decrease row-level access via the **+/-** button on the right, and click **Submit**.

   ![Add Row ACL](#TODO)

6. For granted row-level access, click the **...** button on the right to modify the settings, or click **Delete** on the right.

   ![Edit Row ACL](#TODO)

7. Confirm that the currently settings for Table ACLs are correct and click **Submit**.



### <span id="notice">Notices</span>

- The authorization operation is a whitelist display for the user/user group, that is, only the tables / rows / columns permissions added for the user/user group are accessible.
- By default, a user/user group will be automatically granted all access permissions on all tables in this project after added into this project. You can modify the configuration item `kylin.acl.project-internal-default-permission-granted=true` in the configuration file. After setting it to `false`, when the user/user group is added to the project, there is no table granted by default. System administrators can manually select tables and set access permissions.
- The access to the user/user group takes the largest set of results that can be accessed (union).
  - If user `user1` has access to table `table1`, user `user1` is in user group `group1`, and user group `group1` has access to table `table2`. At this time user `user1` can also access the tables `table1` and `table2`.
  - If user group `group1` has access to tables `table1` and `table2`, user group `group2` has access to tables `table2` and `table3`, user `user1` belongs to user group `group1` and `group2`. At this time `user1` has access to the tables `table1`, `table2` and `table3`.
- Only system administrator users can perform **add/delete/change ** operations at the Table / Column / Row ACL.
