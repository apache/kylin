---
title: Build Index
language: en
sidebar_label: Build Index
pagination_label: Build Index
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - build index
draft: false
last_update:
    date: 08/19/2022
---

As the business scenario changes, some of the indexes in the model need to be retained only in latest months for saving building and storage costs. Therefore, Kylin provides a more flexible way to build indexes since the 5.0 released.


### Build Index

In the **Index Overview** tab, we can see the index list and some basic information. In the **Index List**, we can filter some indexes by the keyword or ids and then only build them in selected segments. For example, some new columns are added in the source table because of the business demands. Therefore, we need to add some new indexes for those columns in the latest one month for analysis and cost saving. As shown in the figure below, we can select all the new and NO BUILD indexes, and then click the **Build Index** button.

![Build Index](images/load_by_date/build_index.png)

After that, please select the segment with the latest month and click the Build Index button to generate the building job. If you want to build the segments concurrently to improve the efficiency, you can also check the **Generate multiple segments in parallel** box. Then, the system will generate multiple jobs according to the number of selected segments.

![Build Index](images/load_by_date/build_index_by_segment.png)

### Delete Index

Similar to the building index, you can also delete some indexes in selected segments. For example, deleting some low frequent usage indexes in last year. As shown below, we can choose some of the indexes and click the **Delete** button to choose delete from all segments or parts of them.

Note: If the indexes are deleted from segments, it may influence the query performance because some of query may route to the pushdown engine due to the lack of index.

![Delete Index](images/load_by_date/delete_index.png)

### Build All Index

To support more flexible index building, it may expect that different indexes will be included in different segments. In order to ensure the stable query performance, we recommend you build all index among all segments after a period of time. Therefore, if the index is incomplete, we can quickly build all indexes by clicking the icon after the index data range.

![Build All Index](images/load_by_date/build_all_index.png)

As shown below, all the segments with incomplete indexes will be shown after clicked the icon. Then, you can select all the segments and click **Build Index** to ensure segments with all indexes.

![Build All Index](images/load_by_date/build_all_index2.png)

