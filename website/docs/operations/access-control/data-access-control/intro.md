---
title: Data Access Control
language: en
sidebar_label: Data Access Control
pagination_label: Data Access Control
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - data access control
draft: true
last_update:
    date: Aug 16, 2022
---


Kylin provides a rich set of access control features for big enterprise. Start from Kylin 5, every action from user must satisfy both **Operation Permission** and **Data Access Permission**, before the action can perform.

- Operation Permission: Defined at project level, specifies what operations a user can perform within a project.  User can have one of the four permissions, from weak to powerful:
  - *QUERY*: Allows to run query in a project.
  - *OPERATION*: Allows to operate models, like building, refreshing, and managing Segments. Implies the QUERY permission.
  - *MANAGEMENT*: Allows to manage models and cubes, like create and edit. Implies the OPERATION permission.
  - *ADMIN*: Project level administrator permission, allows to manage source tables, and all other operations in a project.
  
  See [Project ACL](project_acl.md) for more details.
  
- Data Access Permission: Defined on data, specifies which tables, columns, and rows a user can access. See [Table](acl_table.md) for more details.

###  Examples of Permission Check

To perform an action, user must have both operation permission and data access permission. Below are a few examples.

- To manage source tables, user needs the ADMIN permission, and only the tables user can access can be seen and acted. (Column and row ACLs does not impact the source table management.)
- To edit a model, user must have the MANAGEMENT permission and have access to all the tables and columns in the model.
- Running queries is mostly about data access control, since all users in a project have at least QUERY permission. First user must have access to all the tables and columns in the query, or the system will prompt permission error and refuse to execute. Second the system will only return rows that are accessible to a user. If different row ACLs are set for users, they may see different results from a same query.

### Other Notes

- The system administrator is not restricted by the data access controls by default, he/she has access to all data.
- The system does not provide operation permissions at model level yet.
