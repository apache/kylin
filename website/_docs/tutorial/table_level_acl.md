---
layout: docs
title: Table Level ACL
categories: tutorial
permalink: /docs/tutorial/table_level_acl.html
since: v2.0.0
---

Whether a user can access a table is determined by table-level access control, this function is on by default. Set `kylin.query.security.table-acl-enabled` to false to disable the table-level access control.
Access permissions are independent between different projects.
Once table-level access permission has been set for a user, you can see it on the page.


### Manage Access Permission at Table-level

1. Click the Data Source tab of Model page.
2. Expand a database, choose the table and click Access tab.
3. Click `Grant`to grant permission to user.

	![](/images/Table-level-acl/ACL-1.png)

4. Choose the type (user or role), choose User / Role name and then click `Submit` to grant permission.

5. You can also delete permission on this page.

   ![](/images/Table-level-acl/ACL-2.png)
 
   ​
