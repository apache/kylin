---
layout: docs-cn
title: 表级别权限控制
categories: tutorial
permalink: /cn/docs/tutorial/table_level_acl.html
since: v2.0.0
---

用户是否可以访问表取决于表级别的权限控制，该功能默认开启。可通过将 `kylin.query.security.table-acl-enabled` 的值设为 false 的方式关闭该功能。
不同项目之间权限是互不影响的。
一旦将表权限赋予用户，则该用户可在页面上看到该表。


### 管理表级别权限

1. 点击 Model 页面的 Data Source
2. 展开某个数据库，选择一张表并点击 Access
3. 点击 `Grant` 授权给用户

	![](/images/Table-level-acl/ACL-1.png)

4. 选择 type（有 user 和 role 两种），在下拉框中选择 User / Role name 并点击 `Submit` 进行授权

5. 您也可以在该页面删除该权限。

   ![](/images/Table-level-acl/ACL-2.png) 
   ​
