---
layout: docs31-cn
title: 项目和表级别权限控制
categories: tutorial
permalink: /cn/docs31/tutorial/project_table_level_acl.html
since: v2.1.0
---


## 项目级别权限控制

用户是否可以访问一个项目并使用项目中的功能取决于项目级别的权限控制，Kylin 中共有 4 种默认用户组。分别是 *ADMIN*，*MANAGEMENT*，*OPERATION* 和 *QUERY*。每个用户组对应不同的功能。

- *QUERY*：适用于只需在项目中有查询表/cube 权限的分析师。
- *OPERATION*：该角色适用于需维护 Cube 的公司/组织中的运营团队。OPERATION 包含 QUERY 的所有权限。
- *MANAGEMENT*：该角色适用于充分了解数据/模型商业含义的模型师，建模师会负责模型和 Cube 的设计。MANAGEMENT 包含 OPERATION 和 QUERY 的所有权限。
- *ADMIN*：该角色全权管理项目。ADMIN 包含 MANAGEMENT，OPERATION 和 QUERY 的所有权限。

访问权限是项目隔离的。

### 如何确定访问权限

为用户设置项目级别的访问权限后，不同的角色对应于不同的对数据源，模型和 Cube 的访问权限。具体的功能，以及每个角色的访问权限，如下表所示。

|                                          | System Admin | Project Admin | Management | Operation | Query |
| ---------------------------------------- | ------------ | ------------- | ---------- | --------- | ----- |
| Create/delete project                    | Yes          | No            | No         | No        | No    |
| Edit project                             | Yes          | Yes           | No         | No        | No    |
| Add/edit/delete project access permission | Yes          | Yes           | No         | No        | No    |
| Check model page                         | Yes          | Yes           | Yes        | Yes       | Yes   |
| Check data source page                   | Yes          | Yes           | Yes        | No        | No    |
| Load, unload table, reload table         | Yes          | Yes           | No         | No        | No    |
| View model in read only mode             | Yes          | Yes           | Yes        | Yes       | Yes   |
| Add, edit, clone, drop model             | Yes          | Yes           | Yes        | No        | No    |
| Check cube detail definition             | Yes          | Yes           | Yes        | Yes       | Yes   |
| Add, disable/enable, clone cube, edit, drop cube, purge cube | Yes          | Yes           | Yes        | No        | No    |
| Build, refresh, merge cube               | Yes          | Yes           | Yes        | Yes       | No    |
| Edit, view cube json                     | Yes          | Yes           | Yes        | No        | No    |
| Check insight page                       | Yes          | Yes           | Yes        | Yes       | Yes   |
| View table in insight page               | Yes          | Yes           | Yes        | Yes       | Yes   |
| Check monitor page                       | Yes          | Yes           | Yes        | Yes       | No    |
| Check system page                        | Yes          | No            | No         | No        | No    |
| Reload metadata, disable cache, set config, diagnosis | Yes          | No            | No         | No        | No    |


另外，当查询下压开启时，该项目的查询权限允许用户查询项目中的所有表即使没有 cube 为他服务。每个用户都会被授予查询权限。

### 管理项目级别的访问权限

1. 在 Model 页面，点击左上角的小齿轮形状图标。您将被重定向到项目页面。

   ![](/images/Project-level-acl/ACL-1.png)

2. 在项目页面，展开一个项目并选择 Access。
3. 点击 `Grant` 为用户赋予权限。

	![](/images/Project-level-acl/ACL-2.png)

4. 填写用户或角色的名称，选中权限然后点击 `Grant` 赋予权限。

5. 您也可以在该页面移除或更新权限。

   ![](/images/Project-level-acl/ACL-3-1.png)

   请注意，为了向默认用户（MODELER 和 ANALYST）授予权限，这些用户至少需要登录一次。
   ​

## 表级别权限控制
用户是否可以访问表取决于表级别的权限控制，该功能默认开启。可通过将 `kylin.query.security.table-acl-enabled` 的值设为 false 的方式关闭该功能。
不同项目之间权限是互不影响的。
一旦将表权限赋予用户，则该用户可在页面上看到该表。


### 管理表级别权限

1. 点击 Model 页面的 Data Source
2. 展开某个数据库，选择一张表并点击 Access
3. 点击 `Grant` 授权给用户

	![](/images/Table-level-acl/ACL-1.png)

4. 选择 type（有 user 和 group 两种），在下拉框中选择 User / Group name 并点击 `Submit` 进行授权

5. 您也可以在该页面删除该权限。

   ![](/images/Table-level-acl/ACL-2.png) 
   
   
## 用户和用户组
从 v3.0 开始，Kylin支持用户和用户组的创建。 

用户组是一组用户的集合，用户组中的用户通过用户组共享相同的访问权限。其中 ALL_USERS 组是一个默认的用户组，用户被创建后，即进入该组，也就是说，所有用户都包含在 ALL_USERS 用户组中。
当用户组与用户组中的用户被同时赋予某一项目级／行级／表级／列级权限时，用户的权限取两者最大权限。

![](/images/Project-level-acl/User_Group_Management.png) 

#### 创建用户
所有使用 Kylin 的用户都需要使用账号和对应密码登录。 
在用户管理页面，系统管理员可以点击 "+User"，在弹出框输入账号名(TestAccount)和密码(至少一个字母、一个数字和一个符号，例如abc.1234)来创建新用户。

![](/images/Project-level-acl/Create_User.png) 

#### 创建用户组
在用户组管理页面，系统管理员可以点击 "+Group"，在弹出框输入用户组名来创建新用户组。

![](/images/Project-level-acl/Create_Group.png) 

#### 管理用户   

- 启用、禁用用户

系统管理员可以启用或禁用用户，用户被禁用后将不可再登录系统。

![](/images/Project-level-acl/Disable_User.png) 

- 修改密码

在弹出窗口中，系统管理员可以更改密码。

![](/images/Project-level-acl/Change_Password.png) 

- 删除用户

系统管理员可以弹出窗口中确认删除用户，用户被删除后将不能恢复，删除用户将删除用户在所有项目上的访问权限。

- 分配用户组

将某个用户分配到特定组，可以更加方便地管理对一群用户进行权限管理。

![](/images/Project-level-acl/Assign_User_Group.png) 
![](/images/Project-level-acl/Assign_User_Group_2.png) 

#### 管理用户组

- 分配用户到用户组
- 删除用户组