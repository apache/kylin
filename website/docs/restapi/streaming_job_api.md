---
title: Streaming Job API
language: en
sidebar_label: Streaming Job API
pagination_label: Streaming Job API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - streaming job api
draft: true
last_update:
    date: 08/12/2022
---

> Reminders:
>
> 1. Please read [Access and Authentication REST API](authentication.md) and understand how authentication works.
> 2. On Curl command line, don't forget to quote the URL if it contains the special char `&`.


### Get Job List

- `GET http://host:port/kylin/api/streaming_jobs`

- Introduced in: 4.5.8.2

- Scenarios

  Obtain detailed information of streaming jobs, and provide operation and maintenance decisions based on information such as task status.

- URL Parameters
  - `model_name` - `optional` `string`, fuzzy match model name

  - `model_names` - `optional` `array<string>`, exact model name list

  - `job_types` - `optional` `array<string>`, job types, Optional values:`STREAMING_MERGE`,`STREAMING_BUILD`

  - `statuses` - `optional` `array<string>`, job status, Optional values:`STARTING`,`RUNNING`,`STOPPING`,`ERROR` ,`STOPPED`

  - `project` - `optional` `string`, project name

  - `page_offset` - `optional` `int`, offset of returned result, 0 by default

  - `page_size` - `optional` `int`, quantity of returned result per page, 10 by default

  - `sort_by` -  `optional`  `string`, sort field, optional values: `last_modified` by default,Optional values:`model_alias`、`data_latency`、`last_status_duration`、`last_modified`

  - `reverse` - `optional` `boolean`, whether sort reverse, "true" by default

  - `job_ids` -  `optional`  `array<string>`, job id list, the parameter `project` is required when `job_ids` is not empty

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/streaming_jobs' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Details

  - `Code`: `String`, response code. **Returned value**: `000` (request processing success), `999 ` (request processing failed)
  - `data`: `JSON`, response data.
    - `value`: details of the response data, which consists of: 
      - `uuid`: `String`, job ID
      - `last_modified`: `Long`,  last modified time of the job.
      - `version`: `String`, system metadata version.
      - `mvcc`: `Long`, version number with metadata modified.
      - `model_alias`: `String`, model alias.
      - `owner`: `String`, job owner
      - `model_id`: `String`, Model ID.
      - `last_start_time`: `Long`, last start time of the job.
      - `last_end_time`: `Long`, last end time of the job.
      - `last_update_time`: `Long`, last update time of the job.
      - `last_batch_count`: `Long`, last number of messages processed of the job.
      - `subscribe`: `String`, topic name.
      - `fact_table`: `String`, fact table.
      - `job_status`: `String`, job status, Possible values:`STARTING`,`RUNNING`,`STOPPING`,`ERROR` ,`STOPPED`.
      - `job_type`: `String`, job types, Possible values:`STREAMING_MERGE`,`STREAMING_BUILD`.
      - `process_id`: `String`, the process ID of the job.
      - `node_info`: `String`, the information of the machine that run the job.
      - `job_execution_id`: `String`, the execution ID of the job.
      - `yarn_app_id`: `String`, the ApplicationId on yarn.
      - `yarn_app_url`: `String`, the Application url on yarn.
      - `project`: `String`, project name.
      - `skip_listener`: `Boolean`，whether skip to use listener。
      - `action`: `String`, the action that job is located.
      - `model_broken`: `Boolean`, whether the model is broken or not.
      - `data_latency`: `Long`, the minimum latency of data(ms).
      - `last_status_duration`: `Long`, the last status change time(ms).
      - `model_indexes`: `Int`, total number of indexes.
      - `launching_error`: `Boolean`, whether launching error.
      - `params`: `JSON`, the building parameters of the job.
      - `partition_desc`: `JSON`, partition description.
    - `offset`: page number
    - `limit`: jobs listed in each page
    - `total_size`: total number of jobs
  - `msg`: `String`: error message

- Response Example

  ```json
    {
    "code": "000",
    "data": {
      "value": [
        {
          "uuid": "7bccf62d-535c-70b8-8271-eaef3985aa96_merge",
          "last_modified": 1645186414513,
          "create_time": 1644823384864,
          "version": "4.0.0.0",
          "mvcc": -1,
          "model_alias": "hy_model",
          "owner": "ADMIN",
          "model_id": "7bccf62d-535c-70b8-8271-eaef3985aa96",
          "last_start_time": null,
          "last_end_time": null,
          "last_update_time": "2022-02-18 19:21:27",
          "last_batch_count": null,
          "subscribe": null,
          "fact_table": "BASE.HY_LINEORDER",
          "job_status": "ERROR",
          "job_type": "STREAMING_MERGE",
          "process_id": null,
          "node_info": null,
          "job_execution_id": null,
          "yarn_app_id": "",
          "yarn_app_url": "",
          "params": {
            "spark.executor.memory": "1g",
            "kylin.streaming.segment-max-size": "32m",
            "spark.master": "yarn",
            "spark.driver.memory": "512m",
            "spark.executor.cores": "2",
            "kylin.streaming.job-retry-enabled": "false",
            "spark.executor.instances": "2",
            "kylin.streaming.segment-merge-threshold": "3",
            "spark.sql.shuffle.partitions": "8"
          },
          "project": "p1",
          "skip_listener": false,
          "action": null,
          "model_broken": false,
          "data_latency": null,
          "last_status_duration": 240562073,
          "model_indexes": 3,
          "launching_error": false,
          "partition_desc": {
            "partition_date_column": "HY_LINEORDER.LO_PARTITIONCOLUMN",
            "partition_date_start": 0,
            "partition_date_format": "yyyy-MM-dd HH:mm:ss",
            "partition_type": "APPEND",
            "partition_condition_builder": "org.apache.kylin.metadata.model.PartitionDesc$DefaultPartitionConditionBuilder"
          }
        },
        {
          "uuid": "7bccf62d-535c-70b8-8271-eaef3985aa96_build",
          "last_modified": 1645186414089,
          "create_time": 1644823384857,
          "version": "4.0.0.0",
          "mvcc": -1,
          "model_alias": "hy_model",
          "owner": "ADMIN",
          "model_id": "7bccf62d-535c-70b8-8271-eaef3985aa96",
          "last_start_time": null,
          "last_end_time": null,
          "last_update_time": "2022-02-18 17:43:27",
          "last_batch_count": null,
          "subscribe": null,
          "fact_table": "BASE.HY_LINEORDER",
          "job_status": "ERROR",
          "job_type": "STREAMING_BUILD",
          "process_id": null,
          "node_info": null,
          "job_execution_id": null,
          "yarn_app_id": "application_1643095564973_0592",
          "yarn_app_url": "http://10.1.2.210:8088/proxy/application_1643095564973_0592/",
          "params": {
            "spark.executor.memory": "1g",
            "spark.master": "yarn",
            "spark.driver.memory": "512m",
            "kylin.streaming.kafka-conf.maxOffsetsPerTrigger": "0",
            "kylin.streaming.duration": "30",
            "spark.executor.cores": "2",
            "kylin.streaming.job-retry-enabled": "false",
            "spark.executor.instances": "2",
            "spark.sql.shuffle.partitions": "8"
          },
          "project": "p1",
          "skip_listener": false,
          "action": null,
          "model_broken": false,
          "data_latency": 0,
          "last_status_duration": 246442660,
          "model_indexes": 3,
          "launching_error": false,
          "partition_desc": {
            "partition_date_column": "HY_LINEORDER.LO_PARTITIONCOLUMN",
            "partition_date_start": 0,
            "partition_date_format": "yyyy-MM-dd HH:mm:ss",
            "partition_type": "APPEND",
            "partition_condition_builder": "org.apache.kylin.metadata.model.PartitionDesc$DefaultPartitionConditionBuilder"
          }
        }
      ],
      "offset": 0,
      "limit": 10,
      "total_size": 2
    },
    "msg": ""
  }
  ```



### Operate Job

- `PUT http://host:port/kylin/api/streaming_jobs/status`

- Introduced in: 4.5.8.2

- Scenarios
  
  Modify the status of the jobs. For example, you can restart job task after finding that the job is abnormal.

- URL Parameters
  - `action` - `required` `string`, action types for jobs. Optional values are below:
    - `START`,start selected jobs
    - `STOP`, stop selected jobs
    - `FORCE_STOP`, force to stop the selected jobs
    - `RESTART`, restart selected jobs

  - `project` - `optional` `string`, project name.

  - `job_ids` - `required` `array<string>`, job id.

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl --location --request PUT 'http://host:port/kylin/api/streaming_jobs/status' \
   -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
   -H 'Accept-Language: en' \
   -H 'Authorization: Basic QURNSU46S1lMSU4=' \
   -H 'Content-Type: application/json;charset=utf-8' \
   -d '{
     "project": "p1",
     "action": "RESTART",
     "job_ids": [
     "7bccf62d-535c-70b8-8271-eaef3985aa96_merge"
     ]
   }'
  ```

- Response Details
  - `Code`: `String`, response code. **Returned value**: `000` (request processing success), `999 ` (request processing failed)
  - `data`: `String`, response data, always be empty.
  - `msg`：`String`: error message
  
- Response Example

  ```json
  {
      "code":"000",
      "data":"",
      "msg":""
  }
  ```
