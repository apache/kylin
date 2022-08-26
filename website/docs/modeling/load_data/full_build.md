---
title: Full Load
language: en
sidebar_label: Full Load
pagination_label: Full Load
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - full load
draft: false
last_update:
    date: 08/19/2022
---

If you want to load all the data in the source table, you can choose full load. The main contents are as follows:

- [<span id="expert">Full Load</span>](#full-load-in-ai-augmented-mode-project)

### <span id="expert">Full Load</span>

If you do not set a time partition column for your model, it will be full load each time.

You cannot merge segments in a full load type model since there should be only one segment.

Here we will introduce how to do a full load in the Web UI:

1. Select the model that needs the full load in the model list. Click the **Build Index** button.

   ![Load Data](images/full_load/load_data.png)

2. You will be prompted to load all data, including the data already loaded in the model.

   ![Notice](images/full_load/notice.png)

   > **Note**: If you load data for a model for the first time, the storage size in the above prompt will be 0.00 KB because the model has not been loaded (there is no data in the model). 

3. After that, you can view the build index job via the **Monitor -> Job** page.

4. When the data is loaded, you can view the details in the model list. There is only one Segment in the **Segment** tag, and it is marked as full load.

   ![Full Load](images/full_load/full_load.png)
