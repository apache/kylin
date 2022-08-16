---
title: User Management
language: en
sidebar_label: User Management
pagination_label: User Management
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - user management
draft: true
last_update:
    date: Aug 16, 2022
---


This chapter introduces what a user is and how a user can be managed.

### <span id="user">About Users</span>

To use Kylin, a user must log in to the system using a user name and corresponding password. Every user is unique in a Kylin instance, which is to say, it is not necessary to create the same user for every project in a single instance. 
By default, Kylin initializes one user, namely `ADMIN`. The user `ADMIN` is a built-in system administrator, and the system administrator has all the permissions of the entire system.


### <span id="management">Manage Users</span>

After the system administrator logs in to Kylin, click the **Admin** button in the top toolbar to enter the system management page, and click the **User** field to enter the User Management page.

**Note**:

1. Except for the system administrator, simply creating a user does not give the user access to any project.
2. Except for the system administrator, other users need to be given access at the project level.


### <span id="add">Add a User</span>

On the User Management page, the system administrator can click the **+User** button to add new users. In the pop-up window, please fill in the user name, password,  confirm new password, select whether the user role is a system administrator or a normal user, and click **OK**.

> **tips:** username is case insensitive, so duplicate names with existing user names are not allowed.

### <span id="edit">Edit a User Role</span>

On the User Management page, select a user to be edited, click the **...** (More Actions) button under the **Actions** bar on the right. Then click **Edit Role**. 

In the pop-up window,  the system administrator can modify user role to administrator or user. 

### <span id="drop">Delete a User</span>

On the User Management page, select a user to be deleted, click the **...** (More Actions) button under the **Actions** bar on the right. Then click **Delete**. The system administrator can confirm to delete a user in the prompted window. User can not be restored after deleting, and user's access permission on all projects will be removed.

### <span id="disable">Enable/Disable a User</span>

On the User Management page, select a user, and click the **...** (More Actions) button under the **Actions** bar on the right. Then click **Enable / Disable**. The system administrator can enable or disable a user, and disabled users cannot login to the system. 

### <span id="adminpwd">Reset Password for ADMIN</span>

On the User Management page,  select a user, click **Reset Password** under the **Actions** bar on the right. 

In the pop-up window, the system administrator can change the password and need to enter the new password twice.

The initial ADMIN account password needs to be modified after the first login. To reset the password, you can execute the following command. After successful execution, the ADMIN account will regenerate a random password and display it on the console. When you log in, you need to change the password:

```sh
$KYLIN_HOME/bin/admin-tool.sh admin-password-reset
```

When the parameter `kylin.metadata.random-admin-password.enabled=false`, it will not regenerate a random password but the fixed password `KYLIN`. If the parameter `kylin.metadata.random-admin-password.enabled` is set from `false` to `true` , it will regenerate a random password and display it on the console after all the Kylin nodes restarted.

**Caution** When run this command, Kylin will enter maintenance mode. If the command is interrupted by force, you may need to exit maintain mode manually. Refer to [maintenance_mode](../system-operation/maintenance_mode.en.md).


### <span id="pwd">Reset password for Non-admin</span>

Click  **<username\>**-->**Setup** on the top right corner of the navigation bar. In the pop-up window, user need to provide the old password and repeat the new password twice to reset password.


### <span id="group">Assign a User to a Group</span>

To assign a user to a group, please do the followings:
1. On the User Management page,  select a user to be grouped.
2. Click **Assign to Group** under the **Actions** bar on the right.
3. Select a group to assign the user to under **Candidates**, and then click the right arrow **>**. The group will enter into **Selected**.
4. Click **OK** and the user will be in the selected group.


### <span id="update_group">Modify User Group</span>

To modify user group, please do the following steps:
1. On the User Management page,  select a user to modify the group membership.
2.  Click **Assign to Group** under the **Actions** bar on the right.
3. Select the group to be modified under **Selected**, and then click the left arrow **<**. The group will enter into **Candidates**.
4. Click **OK** and the user group membership will be modified.
