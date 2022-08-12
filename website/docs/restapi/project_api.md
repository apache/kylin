---
title: Project Setting API
language: en
sidebar_label: Project Setting API
pagination_label: Project Setting API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
  - project setting api
draft: false
last_update:
  date: 12/08/2022
---


## Project Setting API

> Reminders:
>
> 1. Please read [Access and Authentication REST API](authentication.md) and understand how authentication works.
> 2. On Curl command line, don't forget to quote the URL if it contains the special char `&`.

### <span id="create-project">Create a Project </span>

- `POST http://host:port/kylin/api/projects`

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object
  - `name` - `required` `string`, project name.
  - `description` - `optional` `string`, project description.
  - `maintain_model_type` - `required` `string`, project type, `MANUAL_MAINTAIN` for AI augmented mode.


- Curl Request Example

  ```sh
  curl -X POST \
  'http://host:port/kylin/api/projects' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{
    "name": "a",
    "description": "",
    "maintain_model_type": "MANUAL_MAINTAIN"
  }'
  ```


- Response Example

  ```json
  {
      "code":"000",
      "data":{
          "uuid":"c7713bea-7df5-49d5-9713-f0094addbafe",
          "last_modified":1574389912687,
          "create_time":1574389912687,
          "version":"4.0.0.0",
          "mvcc":0,
          "name":"a",
          "owner":"ADMIN",
          "status":"ENABLED",
          "create_time_utc":1574389912687,
          "default_database":"DEFAULT",
          "description":"",
          "ext_filters":[
  
          ],
          "maintain_model_type":"AUTO_MAINTAIN",
          "override_kylin_properties":{
  
          },
          "segment_config":{
              "auto_merge_enabled":true,
              "auto_merge_time_ranges":[
                  "WEEK",
                  "MONTH",
                  "QUARTER",
                  "YEAR"
              ],
              "volatile_range":{
                  "volatile_range_number":0,
                  "volatile_range_enabled":false,
                  "volatile_range_type":"DAY"
              },
              "retention_range":{
                  "retention_range_number":1,
                  "retention_range_enabled":false,
                  "retention_range_type":"MONTH"
              }
          }
      },
      "msg":""
  }
  ```



### <span id="delete-project">Delete a Project  </span>

- `DELETE  http://host:port/kylin/api/projects/{project}`

- URL Parameters
  
  - `project` - `required` `string`, project name.
  
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X DELETE \
  'http://host:port/kylin/api/projects/b' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Example

  ```json
  {
      "code":"000",
      "data":null,
      "msg":""
  }
  ```


### General Setting  {#general-setting}

- `PUT http://host:port/kylin/api/projects/{project}/project_general_info`

- URL Parameters
  
  - `project` - `required` `string`, project name.
  
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object
  - `description` - `optional` `string`, project description.
  - `semi_automatic_mode` - `optional` `boolean`, whether to turn 
     on Recommendation Mode. `True` means to turn on, `false` means to turn off. The default value is `false`.
  
- Curl Request Example

  ```sh
  curl -X PUT \
  'http://host:port/kylin/api/projects/b/project_general_info' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{
    "description":"description", 
    "semi_automatic_mode":true
  }'
  ```

- Response Example

  ```json
  {
      "code":"000",
      "data":null,
      "msg":""
  }
  ```


### Storage Quota  {#storage-quota}

- `PUT http://host:port/kylin/api/projects/{project}/storage_quota`

- URL Parameters
  
  - `project` - `required` `string`, project name.
  
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object
  
- `storage_quota_size` - `required` `long`, project storage quota, unit byte. Optional values: positive numbers and be equal or greater than `1099511627776` , which means equal or greater than 1 TB.
  
- Curl Request Example

  ```sh
  curl -X PUT \
  'http://host:port/kylin/api/projects/b/storage_quota' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{
      "storage_quota_size": 1099511627776
  }' 
  ```

- Response Example

  ```json
  {
      "code":"000",
      "data":null,
      "msg":""
  }
  ```


### Low Usage Storage  {#inefficient-storage}

- `PUT http://host:port/kylin/api/projects/{project}/garbage_cleanup_config`

- URL Parameters
  - `project` - `required` `string`, project name.


- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object
  - `frequency_time_window` - `required` `string`, low usage storage calculation period. Optional values: `MONTH`, `WEEK`, `DAY`.
  - `low_frequency_threshold` - `required` `int`, the number of use of low usage storage. Optional values: positive integer and `0`. For example, you can set when usage is lower than 10 times, then the storage of indices would be regarded as low usage storage.

- Curl Request Example

  ```sh
  curl -X PUT \
  'http://host:port/kylin/api/projects/b/garbage_cleanup_config' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{
      "frequency_time_window": "WEEK", 
      "low_frequency_threshold": 7
  }' 
  ```

- Response Example

  ```json
  {
      "code":"000",
      "data":null,
      "msg":""
  }
  ```



### <span id="Pushdown">Pushdown  Setting</span>

- `PUT http://host:port/kylin/api/projects/{project}/push_down_config`

- URL Parameters
  
  - `project` - `required` `string`, project name.
  
- HTTP Header
  - `Content-Type: application/json;charset=utf-8`
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en` 

- HTTP Body: JSON Object
  
  - `push_down_enabled` - `required` `boolean`, whether to turn on query pushdown, `true` for turning on, `false` for turning off.
  
- Curl Request Example

  ```sh
  curl -X PUT \
  'http://host:port/kylin/api/projects/b/push_down_config' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{
   "push_down_enabled":true
  }'
  ```

- Response Example

  ```json
  {
      "code":"000",
      "data":null,
      "msg":""
  }
  ```



### <span id="pushdown-config">Pushdown Configure Setting</span>
  
- `PUT http://host:port/kylin/api/projects/{project}/push_down_project_config`

- URL Parameters
  
  - `project` - `required` `string`，project name。
  
- HTTP Header
  - `Content-Type: application/json;charset=utf-8`
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en` 

- HTTP Body: JSON Object
  
- `runner_class_name` - `required` `string`，project config property `kylin.query.pushdown.runner-class-name`. To specify the query engine when query pushdown. For default, when pushdown to the native Spark, the value is `org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl`.
- `converter_class_names` - `required` `string`，project config SQL conversion property `kylin.query.pushdown.converter-class-names`.
  
- Curl Request Example

  ```sh
  curl -X PUT \
  'http://host:port/kylin/api/projects/b/push_down_project_config' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{
   "project": "project_name",
   "runner_class_name":"org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl",
   "converter_class_names":"org.apache.kylin.query.security.HackSelectStarWithColumnACL,org.apache.kylin.query.util.SparkSQLFunctionConverter"
  }' 
  ```

- Response Example

  ```json
  {
      "code":"000",
      "data":null,
      "msg":""
  }
  ```



### Segment Settings  {#segment-setting}

- `PUT http://host:port/kylin/api/projects/{project}/segment_config`

- URL Parameters
  
  - `project` - `required` `string`, project name.
  
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object
  - `auto_merge_enabled` - `required` `boolean`, whether to turn on segment auto-merge, `true` for turning on, `false` for turning off.
  - `auto_merge_time_ranges` - `required` `array`, the period of segment auto-merge and the unit of retentio threshold. Optional values: `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR`. For example, you can set auto-merge the segments in 1 week.
  - `volatile_range` - `optional` `json`, the volatile range of auto-merge, which means 'Auto-Merge' will not merge latest [Volatile Range] days segment. For example, If you set 10 days, it means that segments within 10 days will not be merged automatically.
    - `volatile_range_number` - `optional` `int`，The time of volatile range. The default value is `0`. Optional values: positive integer and `0`.
    - `volatile_range_type` - `optional` `string`，The time unit of the volatile range. The default value is `Day`. Optional values: `DAY`, `WEEK`, `MONTH`.
  - `retention_range` - `optional` `json`, retention threshold, which means to retain the segments in the retention threshold. If you set 1 year, that means the segments exceed the 1 year will be removed by the system automatically.
    - `retention_range_enabled` - `optional` `boolean`, whether to turn on the retention threshold, `true` for turning on, `false` for turning off, and the default value is `false`. 
    - `retention_range_number` - `optional` `int`，The time to retention threshold. The default value is `1`. Optional values: positive integer and `0`.
    - `create_empty_segment_enabled` - `optional` `boolean`, whether to allow add new segment.  `true` for turning on, `false` for turning off, and the default value is `false`. 
  
- Curl Request Example

  ```sh
  curl -X PUT \
  'http://host:port/kylin/api/projects/b/segment_config' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{
          "auto_merge_time_ranges":[
              "WEEK",
              "MONTH",
              "QUARTER",
              "DAY",
              "YEAR"
          ],
          "auto_merge_enabled":true,
          "volatile_range":{
              "volatile_range_number":0,
              "volatile_range_type":"DAY"
          },
          "retention_range":{
              "retention_range_number":2,
              "retention_range_enabled":false
          }
      }'
  ```
 > **Notes：** 
  >
  > - `auto_merge_time_ranges` : array contains at least one value.
  > - The time unit of `retention_range` is equal to the maximum time unit in the `auto_merge_time_ranges` array. If `"auto_merge_time_ranges":["DAY",MONTH"]`, then the time unit of `retention_range` is `MONTH`. If `"auto_merge_time_ranges":["YEAR",WEEK"]`, then the time unit of `retention_range` is `YEAR`.                                                               

- Response Example

  ```json
    {
        "code":"000",
        "data":null,
        "msg":""
    }
  ```
  
 
  
### Default Database  {#default-database}

- `PUT http://host:port/kylin/api/projects/{project}/default_database`

- URL Parameters
  
  - `project` - `required` `string`, project name.
  
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object
  
- `default_database` - `required` `string`, default database name.
  
- Curl Request Example

  ```sh
  curl -X PUT \
  'http://host:port/kylin/api/projects/b/default_database' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{
          "default_database":"EDW"
      }'
  ```

- Response Example

  ```json
    {
        "code":"000",
        "data":null,
        "msg":""
    }
  ```


### Job Notification  {#job-notification}

- `PUT http://host:port/kylin/api/projects/{project}/job_notification_config`

- URL Parameters
  
  - `project` - `required` `string`, project name.
  
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object
  - `data_load_empty_notification_enabled` - `required` `boolean`, whether to turn on empty data load notification. `True` means to notify user if there is an empty data load job.
  - `job_error_notification_enabled` - `required` `boolean`, whether to turn on error job notification. `True` means to notify user if there is an error job.
  - `job_notification_emails` - `required` `array`，email address for the job notification. Email format: `xx@xx.xx`.
  
- Curl Request Example

  ```sh
  curl -X PUT \
  'http://host:port/kylin/api/projects/b/job_notification_config' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{
          "data_load_empty_notification_enabled":true,
          "job_error_notification_enabled":false,
          "job_notification_emails":[
              "nnnn@kyligence.io","tttt@kyligence.io"
          ]
      }'
  ```

- Response Example

  ```json
    {
        "code":"000",
        "data":null,
        "msg":""
    }
  ```

### <span id="yarn">YARN Application Queue</span>

- `PUT http://host:port/kylin/api/projects/{project}/yarn_queue`

- URL Parameters

  - `project` - `required` `string`, project name.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object

  - `queue_name` - `required` `string`, the name of YARN application queue.

- Curl Request Example

  ```sh
  curl -X PUT \
  'http://host:port/kylin/api/projects/b/yarn_queue' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{
          "queue_name":"yarnqueue"
      }'
  ```

- Response Example

  ```json
  {
      "code": "000",
      "data": null,
      "msg": ""
  }
  ```



### <span id="update-job-engine">Update the Linking Relationship between Projects and Job Engines</span>

**Note:** This Rest API need to cooperate with multi-active job engines. For more details, please refer to [Kylin Multi-Active Job Engines](../../../installation/deploy/cluster_lb.en.md).

- `POST http://host:port/kylin/api/epoch`

- HTTP Body: JSON Object

  - `projects` - `required` `string`, projects name. If the array is empty, it will update the relationship for all projects
  - `force` - `required` `boolean`, whether to update the relationship directly. For `false`, it will check whether the relationship is outdated. If it is outdated, the system will update it based on current projects and job engines. For `true`, the system will skip the check step and update directly.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X POST \
    'http://host:port/kylin/api/epoch' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{"projects":["Project_A"],"force":true}'
  ```


- Response Example

  ```json
  {
      "code": "000",
      "data": "",
      "msg": ""
  }
  ```

### Project Reset Setting  {#project-reset-setting}

The Low Usage Storage, Segment Settings, Storage Quota and Job Notification can be reset to the default values. Only one of the settings can be reset each time.

- `PUT http://host:port/kylin/api/projects/{project}/project_config`

- URL Parameters
  
  - `project` - `required` `string`, project name.
  
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object
  
  - `reset_item` - `required` `string`, reset project settings item. Optional values: `job_notification_config`, `query_accelerate_threshold`, `garbage_cleanup_config`, `segment_config`, `storage_quota_config`.
  
- Curl Request Example

  ```sh
  curl -X PUT \
  'http://host:port/kylin/api/projects/a/project_config' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{
          "reset_item":"job_notification_config"
      }'
  ```

- Response Example

  ```json
   {
       "code":"000",
       "data":{
           "project":"a",
           "description":"",
           "maintain_model_type":"MANUAL_MAINTAIN",
           "default_database":"EDW",
           "semi_automatic_mode":false,
           "storage_quota_size":10995116277760,
           "push_down_enabled":true,
           "auto_merge_enabled":true,
           "auto_merge_time_ranges":[
               "WEEK",
               "MONTH",
               "QUARTER",
               "YEAR"
           ],
           "volatile_range":{
               "volatile_range_number":0,
               "volatile_range_enabled":true,
               "volatile_range_type":"DAY"
           },
           "retention_range":{
               "retention_range_number":1,
               "retention_range_enabled":false,
               "retention_range_type":"YEAR"
           },
           "job_error_notification_enabled":false,
           "data_load_empty_notification_enabled":false,
           "job_notification_emails":[
   
           ],
           "threshold":20,
           "tips_enabled":true,
           "frequency_time_window":"MONTH",
           "low_frequency_threshold":5
       },
       "msg":""
   }
  ```
  
- The default values for Low Usage Strage:
  - `"frequency_time_window":"MONTH"`
  - `"low_frequency_threshold":0`
- The default values for Segment Settings:
  - `"auto_merge_enabled":true` 
    - `"auto_merge_time_ranges":["DAY", "MONTH", "QUARTER", "YEAR"]`
  - `"volatile_range"` 
    - `"volatile_range_number":0`
    - `"volatile_range_type":"DAY"`
  - `retention_range`
    - `"retention_range_enabled":false` 
    - `"retention_range_number":1` 
- `"create_empty_segment_enabled":false`

- The default values for Job Notification:
  - `"data_load_empty_notification_enabled":false` 
  - `"job_error_notification_enabled":false` 
  - `"job_notification_emails"` is null

- The default values for Storage Quota
  
  - `"kylin.storage.quota-in-giga-bytes": 10240`
