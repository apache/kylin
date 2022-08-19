---
title: Tool Bar
language: en
sidebar_label: Tool Bar
pagination_label: Tool Bar
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - toolbar
draft: true
last_update:
    date: Aug 19, 2022
---

The toolbar of Kylin system is as shown in the following figure.

![Toolbar](images/toolbar.png)


### <span id="project_list">Project List</span>

Project list is located on the left side of the toolbar, and the current project is selected. If it is the first time to start Kylin, current project would be empty. The icon on the left side of this list is used for expand and collapse the navigation bar. The button of `+` on the right side is used for adding new project. The newly added project will be set as the current project.

### <span id="storage_quota">Storage Quota</span>

Click **Storage Quota** for an overview of quota usage.

![存储配额](images/storage_quota.png)

- **Used Storage**: The percentage of storage quota consumed for this project. The values enclosed in parentheses represent the storage quota consumed and total storage quota allocated. By default, the total storage quota is 10 TB. To adjust the default value, see [Project Settings](project_settings.md).

  The stored data mainly consists of model and index data, build job outputs, and dictionary files, etc. For more information, see [Capacity Billing](../capacity_billing.en.md).

- **Low Usage Storage**: Indexes that are less frequently queried within the set time window. By default, indexes that are queried less than 5 times in one month would be considered as low usage storage. To modify this rule, see [Project Settings](project_settings.md).

  > [!NOTE]
  >
  > You can manually clean low usage storage (with [Tutorial](../../quickstart/expert_mode_tutorial.md) enabled) or set scheduled cleaning task. For more information, see [Junk File Clean](../system-operation/junk_file_clean.md). 

### <span id="storage_quota">Service State</span>

The colored dot indicates the status of the service status for the current project. Clicking on it will prompt a dialog shown as follow. The first line in the figure shows the current time of the system. The second line shows the percentage of data used.The third line shows the currently used node information, and the icon on the right `>` can expand to view the detailed node information. 

![Service State](images/service_state.png)


### <span id="system_management">System Management</span>

After the system admin logs into Kylin, click **Admin** on the top bar to enter the administration mode. For details, please refer to the [Project Management](project_management.md) chapter.

### <span id="user_info">User Information</span>

The current login user information is displayed on the far right. Click to modify the password and log out.
