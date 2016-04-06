---
layout: docs15-cn
title:  Kylin Cube 权限授予教程
categories: 教程
permalink: /cn/docs15/tutorial/acl.html
version: v1.2
since: v0.7.1
---

  

在`Cubes`页面，双击cube行查看详细信息。在这里我们关注`Access`标签。
点击`+Grant`按钮进行授权。

![]( /images/Kylin-Cube-Permission-Grant-Tutorial/14 +grant.png)

一个cube有四种不同的权限。将你的鼠标移动到`?`图标查看详细信息。

![]( /images/Kylin-Cube-Permission-Grant-Tutorial/15 grantInfo.png)

授权对象也有两种：`User`和`Role`。`Role`是指一组拥有同样权限的用户。

### 1. 授予用户权限
* 选择`User`类型，输入你想要授权的用户的用户名并选择相应的权限。

     ![]( /images/Kylin-Cube-Permission-Grant-Tutorial/16 grant-user.png)

* 然后点击`Grant`按钮提交请求。在这一操作成功后，你会在表中看到一个新的表项。你可以选择不同的访问权限来修改用户权限。点击`Revoke`按钮可以删除一个拥有权限的用户。

     ![]( /images/Kylin-Cube-Permission-Grant-Tutorial/16 user-update.png)

### 2. 授予角色权限
* 选择`Role`类型，通过点击下拉按钮选择你想要授权的一组用户并选择一个权限。

* 然后点击`Grant`按钮提交请求。在这一操作成功后，你会在表中看到一个新的表项。你可以选择不同的访问权限来修改组权限。点击`Revoke`按钮可以删除一个拥有权限的组。
