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
draft: false
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

###  Examples of Permission Check

To perform an action, user must have operation permission. Below is few examples.

- To edit a model, user must have the MANAGEMENT permission and have access to all the tables and columns in the model.

### Other Notes

- The system administrator is not restricted by the data access controls by default, he/she has access to all data.
- The system does not provide operation permissions at model level yet.
