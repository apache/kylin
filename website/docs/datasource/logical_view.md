---
title: Logical View
language: en
sidebar_label: Logical View
pagination_label: Logical View
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - logical view
draft: false
last_update:
    date: 02/28/2023
---

## Logical View
- Logical view is a special view maintained only in KYLIN rather than a real view in hive. Once created, you can use it like a normal Hive view.
- Logical view feature is turned off by default, you can set `kylin.source.ddl.logical-view.enabled=true` to turn this feature on.
- All logical views are in the same database, you can set the database name by the following parameters, be careful not to have the same name as the normal Hive database:
  - `kylin.source.ddl.logical-view.database=KYLIN_LOGICAL_VIEW`

### Main operation flow
1. Log in to KYLIN, and the login account must be:
- System admin.
- Have the Administrator or Project Administrator role for the target project.
- User has Management Role.

2. Click Data Assets - > Logical View to go to the main page of the logical view, you can enter SQL statements in the text box, and there are three tabs which can respectively manage the logical view tables, load the data source and display the syntax rules. 

3. Enter the `Create Logical View as...` statement in the SQL text box to create the logical view, noting that the database name is not required.
4. After the view is created, you need to load the source table for Modeling to use. Since the logical view is newly created, you need to click "Refresh Now" first, then find the newly created logical view and then complete the loading. 
   

### Delete logical view
1. On the main page of the logical view, you can enter SQL statement on the left: `DROP LOGICAL VIEW logical_view_db.your_logical_view;` to delete the logical view. Pay attention that the database name is needed.


### View/Edit Logical View
1. In the main interface of the logical view, click the 'Logical View Chart' (L) tab, you can see all the created logical views, those of current project will be pinned to the top, and those belonging to other projects will be grey out.
2. Click the pencil icon next to the view to enter view/edit page.

3. On this page, you can view the create view statement.
4. To edit the table definition, click the 'create - > replace' icon

5. You can see that `Create Logical View` in the edit box is replaced with `Replace Logical View`, and then you can modify the previous create table statement. In the following example, the condition of `where 1 = 1` is added.

6. Click Save to complete the editing of the logical view.

### Precautions
1. Logical views must be created under the same virtual database. (The database name can be determined through the parameter above.)
2. In order to prevent users from overriding existing project permissions with the help of logical views, the source table used in logical views needs to be loaded into the data source, and users can only load/edit logical views created under the same project.
3. Logical views currently do not support RDBMS JDBC data sources. It only support Hive data sources.