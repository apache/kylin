---
title: Error Code
language: en
sidebar_label: Error Code
pagination_label: Error Code
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - error code
draft: true
last_update:
    date: 08/12/2022
---

[comment]: <#TODO: renew the error code> (#TODO)

If an error occurs when calling the Kylin API or using the built-in tools, you can troubleshoot the problem based on the error codes and messages. This article summarizes the error codes and error messages for the APIs and built-in tools.
### API error code  
#### Project  
| Error&nbsp; code | Error message                                                                                                |
|:-----------------|:-------------------------------------------------------------------------------------------------------------|
|  KE-010001201    |  Can't find project. Please check and try again.                                                             |
|  KE-010001208    |  Can't use, the multilevel partitioning is not enabled for the project. Please turn it on and try again.     |  

#### Model    
| Error code                | Error message                                                                   |
|:--------------------------|:--------------------------------------------------------------------------------|
|   KE-010002201            |   Can't find model. Please check and try again.                                 |
|   KE-010002202            |   Can't find model id. Please check and try again.                              |
|   KE-010002203            |   Can't find model name. Please check and try again.                            |
|   KE-010002204            |   The name can't be empty.                                                      |
|   KE-010002205            |   The model name is invalid. Please use letters, numbers and underlines only.   |
|   KE-010002206            |   Model already exists. Please rename it.                                       |  

#### Authentication  
| Error code   | Error message                                                |
| :----------- | :----------------------------------------------------------- |
| KE-010003207 | Can't authenticate. Please login again.                      |
| KE-010003208 | Invalid username or password. Please check and try again.    |
| KE-010003209 | Can't find authentication information.                       |
| KE-040005201 | Can't find PASSWORD ENCODER. Please check configuration item <br />kylin.security.user-password-encoder. |
| KE-040005202 | Can't initialize PASSWORD ENCODER. Please check configuration item <br />kylin.security.user-password-encoder. |

#### Index  
|   Error code     | Error message                                                                                                       |
|:-----------------|:--------------------------------------------------------------------------------------------------------------------|
|   KE-010012201   |   Index metadata might be inconsistent. Please try refreshing all segments in the following model.                  |
|   KE-010012202   |   Can’t add this index, as the same index already exists. Please modify.                                            |
|   KE-010012203   |   The parameter is not supported.                                                                                   |  

#### Segment  
| Error code   | Error message                                                |
| :----------- | :----------------------------------------------------------- |
| KE-010022201 | Can't refresh. The segment range exceeds the range of loaded data. Please modify and <br />try again. |
| KE-010022202 | Can't build segment. The specified data range overlaps with the built segments. Please <br />modify and try again. |
| KE-010022203 | Can‘t refresh. Please select segment and try again.          |
| KE-010022204 | Can‘t refresh, some segments are being built. Please try again later. |
| KE-010022205 | Can‘t refresh, the selected segment range is empty. Please reselect and try again. |
| KE-010022206 | Please select at most one segment to refresh.                |
| KE-010022207 | Please select at least two segment to merge.                 |
| KE-010022208 | Can't merge the selected segments, as there are gap(s) in between. Please check and try again. |
| KE-010022209 | Can't find the segment by name. Please check and try again.  |
| KE-010022210 | Can't find the segment by ID. Please check and try again.    |
| KE-010022211 | Can't find the segment(s). Please check and try again.       |
| KE-010022212 | You should choose at least one segment!                      |
| KE-010022213 | You should choose at least one segment to drop!              |
| KE-010022214 | Can't enter segment ID and name at the same time. Please re-enter. |
| KE-010022215 | Please enter segment ID or name.                             |
| KE-010022216 | Can't remove, refresh or merge segment, as it's LOCKED. Please try again later. |
| KE-010022217 | Can't refresh or merge segment in current status. Please try again later. |
| KE-010022218 | The indexes included in the selected segments are not consistent. Please build index first <br />and try merging again. |
| KE-010022219 | The partitions included in the selected segments are not consistent. Please build the<br /> partitions first and try merging again. |

#### Job
| Error code   | Error message                                                |
| :----------- | :----------------------------------------------------------- |
| KE-010032201 | Can't add the job, as the subpartition value is empty. Please check and try again. |
| KE-010032202 | Can't submit the job, as no index is included in the segment. Please check and try again. |
| KE-010032203 | Can't submit the job, as there are no segments in READY status. Please try again later. |
| KE-010032204 | Can't find executable jobs at the moment. Please try again later. |
| KE-010032205 | Can't add the job. Please ensure that the operation is valid for the current object. |
| KE-010032206 | Can't submit the job at the moment, as a building job for the same object already exists.<br /> Please try again later. |
| KE-010032207 | Can't submit the job, as the indexes are not identical in the selected segments. Please<br /> check and try again. |
| KE-010032208 | Can't submit the job to this node, as it’s not a job node. Please check and try again. <br/> |
| KE-010032209 | Can't add the job, as there are duplicate subpartition values. Please check and try again. |
| KE-010032210 | No index is available to be refreshed. Please check and try again. |
| KE-010032211 | Can’t perform job in current status.                         |
| KE-010032212 | The selected job status is invalid. The value of status must be "PENDING", "RUNNING", <br />"FINISHED", "ERROR” or "DISCARDED". Please check and try again. |
| KE-010032213 | At least one job should be selected.                         |
| KE-010032214 | The number of sampling rows are out of range. Please modify it. |
| KE-010032215 | Can't perform current operation, as an error occurred when updating the job status. |
| KE-010032216 | Can't submit building jobs, as it exceeds the concurrency limit. Please try submitting <br />fewer jobs at a time. |
| KE-010032217 | Invalid value in parameter "action" or "statuses" or "job_ids", The value of "statuses" or the<br /> status of jobs specified by "job_ids", this status of jobs can only perform the following actions. |
| KE-010032218 | No storage quota available. The system failed to submit building job, while the query engine <br />will still be available. Please clean up low-efficient storage in time, increase the low-efficient <br />storage threshold, or notify the administrator to increase the storage quota for this project. |
| KE-010032219 | Can't find job. Please check and try again.                  |


#### Parameter Check

| Error code   | Error message                                                |
| :----------- | :----------------------------------------------------------- |
| KE-010043201 | Request parameter is empty or value is empty. Please check the request parameters are <br />filled in correctly. |
| KE-010043202 | The entered parameter value is invalid. The timestamp of the start time and end time<br /> conversion must be greater than or equal to 0. Please check and try again. |
| KE-010043203 | The entered parameter value is invalid. The end time must be greater than the start time. <br />Please check and try again. |
| KE-010043204 | The entered parameter value is invalid. The end time must be greater than or equal to <br />the start time. Please check and try again. |
| KE-010043205 | The entered parameter format is invalid. The timestamp format must be milliseconds. <br />Please check and try again. |
| KE-010043206 | The entered parameter value is invalid. The start time and end time must exist or empty at<br /> the same time. Please check and try again. |
| KE-010043207 | The entered parameter is invalid, only supports specific values at the moment. Please check <br />and try again. |
| KE-010043208 | The entered parameter value is invalid. The parameter value must be a non-negative <br />integer. Please check and try again. |
| KE-010043209 | The entered parameter value is invalid. Only support specific values at the moment. Please <br />check and try again. |
| KE-010043210 | The parameter can't be empty. Please enter the time partition column format. |
| KE-010043211 | The type of the time partition column is invalid. Please enter the supported format, refer <br />to the [user manual](../../Designers-Guide/model/model_design/data_modeling.en.md#faq). |
| KE-010043212 | The parameter can't be empty. Please enter layout(s) id.     |
| KE-010043213 | Can't find layout. Please check and try again.               |
| KE-010043214 | Can't refresh the value, the time units are only supported in d (days), h (hours), <br />or m (minutes). Please check and try again. |
| KE-010043215 | Can't modify this configuration. Please contact admin.       |
| KE-010043216 | Can't delete this configuration. Please contact admin.       |


#### System
|  Error code    | Error message                                                               |
|:---------------|:----------------------------------------------------------------------------|
|  KE-040021201  |  System is trying to recover service. Please try again later.               |
|  KE-040023201  |  Can't execute this request on job node. Please check and try again.        |
|  KE-040023202  |  Job node is unavailable for queries. Please select a query node.           |
|  KE-040023203  |  Can't execute this request on query node. Please check and try again.      |
|  KE-040024201  |  Can't operate Metadata at the moment, as system is in maintenance mode.    |
|  KE-040024202  |  System is already in maintenance mode.                                     |
|  KE-040024203  |  System is not in maintenance mode.                                         |
|  KE-040025201  |  System profile abnormal data.                                              |  

#### Other
| Error code   | Error message                                                |
| :----------- | :----------------------------------------------------------- |
| KE-010007204 | The data type of column from the source table has changed. Please remove the column from <br />model , or modify the data type. |
| KE-010007208 | The table metadata can't be reloaded now. There are ongoing jobs with the following target <br />subjects(s). Please try reloading until all the jobs are completed, or manually discard the jobs. |
| KE-010025201 | Can't find the cube.                                         |
| KE-010031201 | Can’t get query result, as the rows of query result exceeds the maximum limit. Please add <br />filters, or contact admin to adjust the maximum limit. |
| KE-030001201 | Can't build, the expected number of rows for index does not match the actual built number of rows. |
| KE-060100201 | An exception occurred outside Kylin.          |


### Built-in tools error code  
> [!NOTE]
>  Build-in tools include [junk file clean tools](../../Operation-and-Maintenance-Guide/junk_file_clean.en.md), [diagnostic package generate tools](../../Operation-and-Maintenance-Guide/system-operation/cli_tool/diagnosis.en.md), etc.

|    Error code      | Error message                                                                         |
|:-------------------|:--------------------------------------------------------------------------------------|
|    KE-050040201    |    Required parameter is empty.                                                       |
|    KE-050040202    |    Required parameter is not specified.                                               |
|    KE-050040203    |    Required parameter is not specified (milliseconds).                                |
|    KE-050040204    |    Parameter "-endTime" &lt;= Parameter "-startTime".                                 |
|    KE-050041201    |    The path does not exist.                                                           |
|    KE-050041202    |    The path already exists.                                                             |  
