---
title: User Group Management
language: en
sidebar_label: User Group Management
pagination_label: User Group Management
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - user group management
draft: false
last_update:
    date: Aug 16, 2022
---

This chapter provides an overview of what a user group is and how a user group can be managed. 

### <span id="group">About User Group</span>

A user group is a collection of users, and users in a user group share the same ACL. By default, Kylin initializes four user groups, namely **ALL_USERS**, **ROLE_ADMIN**, **ROLE_ANALYST**, and **ROLE_MODELER**. And **ALL_USERS** group is a default user group, all users are included in the **ALL_USERS** user group. **ALL_USERS** and **ROLE_ADMIN** user group cannot be modified or deleted. System administrators can add or remove users in user groups except ALL_USERS, or add a user to multiple groups except **ALL_USERS**. User groups cannot be renamed throughout the Kylin instance.


### <span id="authority">About User Group Permissions</span>

The system administrator can grant the project-level access permissions to a user group. When a user group has been granted the project-level permissions, users in this group will inherit the corresponding permissions from the group.

When a user belongs to multiple groups, the user will inherit the project-level permissions from the groups he/she belongs to. 

### <span id="management">Manage user groups</span>

After the system administrator logs in to Kylin, click the **Admin** button in the top toolbar to enter the system management page, and click the **Group** field to enter the User Group Management page.

### <span id="add">Create a user group</span>

On the User Group Management page,  click **+ User Group** button to create a new group. In the pop-up window, the system administrator can fill in the group name and click **OK** to save a new user group. 

### <span id="drop">Delete a user group</span>

On the User Group Management page, select a user to be deleted, click the **Drop** button under the **Actions** bar on the right. In the pop-up window, the system administrator can confirm to delete a user group, once a user group is deleted, users in this user group will not be deleted and permission grant to this user group will be removed.

### <span id="assign">Assign users to a user group</span>

1. On the User Group Management page, select the user group to be assigned users to.
2. Click **Assign Users** under the **Actions** bar on the right.
3. In the pop-up window, check the users who need to be assigned to the group, click the right arrow **>**, the user will be assigned to the **Assigned Users**.
4. Click **OK** and the user will be assigned to this group.

### <span id="update_group">Modify user's user group</span>

Please refer to [User Management](user_management.md).

