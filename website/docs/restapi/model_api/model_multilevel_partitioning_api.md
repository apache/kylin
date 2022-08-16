---
title: Multi-level Partitioning Model API
language: en
sidebar_label: Multi-level Partitioning Model API
pagination_label: Multi-level Partitioning Model API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - multi-level partitioning model api
draft: false
last_update:
    date: 08/12/2022
---

> Reminders:
>
> 1. Please read [Access and Authentication REST API](../authentication.md) and understand how authentication works.
> 2. On Curl command line, don't forget to quote the URL if it contains the special char `&`.


### Set partition column {#Set partition column}
For multi-level partition model define partition column, please refer to [Define Partition Column](model_management_api.md) directly.

### Add Sub-Partition value {#Add Sub-Partition value}
You can add it by specifying `sub_partition_values` when [Load Segment](model_build_api.md) directly or execute following API.

- `POST http://host:port/kylin/api/models/{model_name}/segments/multi_partition/sub_partition_values`

- URL Parameters
  
  - `model_name` - `required` `string`,model name.

- HTTP Body: JSON Object

  - `project` - `required` `string`,project name.
  
  - `sub_partition_values` - `required` `array`,sub-partition values.
  
- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example
  
  ```sh
  curl -X POST \
      http://host:port/kylin/api/models/multi_level_partition/segments/multi_partition/sub_partition_values \
      -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
      -H 'Authorization: Basic QURNSU46S1lMSU4=' \
      -H 'Content-Type: application/json;charset=utf-8' \
      -d '{
        "project":"multi_level_partition",
        "sub_partition_values":[["5"]]
    }'
  ```
  
- Response Example

  ```json
  {
       "code": "000",
       "data": "",
       "msg": ""
  }
  ```

### Load Segment {#Load Segment}
For building multi-level partition model, please refer directly to [Load Segment API](model_build_api.md).

### Build Sub-Partition {#Build Sub-Partition}

- `POST http://host:port/kylin/api/models/{model_name}/segments/multi_partition`

- URL Parameters
  
  - `model_name` - `required` `string`,model name.

- HTTP Body: JSON Object

  - `project` - `required` `string`,project name.
 
  - `segment_id` - `required` `string`,Segment id.
  
  - `sub_partition_values` - `required` `array`,sub-partition values.
  
  - `parallel_build_by_segment` - `optional` `boolean`, whether to build concurrently,the default value is `false`.
  
  - `build_all_sub_partitions` - `optional` `boolean` whether to build all sub-partition values,the default value is `false`.

  - `yarn_queue` - `optional` `string`, specify the YARN queue used by the job, it can be set after these two parameters were set: kylin.engine-yarn.queue.in.task.enabled (whether to allow set specified YARN queue for build task, default value is false), kylin.engine-yarn.queue.in.task.available (available YARN queues, separate them with English commas)

  - `tag` - `optional` `object`, job tag, if the field is set, when calling the [Get Job List](../job_api.md) API, the field will be the same back when returning the job. It can be used for system integration, mark the job and deal with it accordingly. By default, the maximum size of value is 1024 KB , which can  be set by the configure kylin.job.tag-max-size=1024.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example
  
  ```sh
  curl -X POST \
    http://host:port/kylin/api/models/multi_partition_model/segments/multi_partition \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{
      "project": "multi_partition_project",
      "segment_id": "983904fa-d573-4944-acb8-558c59598a48",
      "sub_partition_values": [
          [
              "5"
          ]
      ],
      "parallel_build_by_segment": false,
      "build_all_sub_partitions": false
  }'
  ```
  
- Response Example

  ```json
  {
      "code": "000",
      "data": {
          "jobs": [
              {
                  "job_name": "SUB_PARTITION_BUILD",
                  "job_id": "0282e2c9-63e3-46b0-85f6-6e74ca5bf984"
              }
          ]
      },
      "msg": ""
  }
  ```

### Delete Sub-Partition value corresponding index data {#Delete Sub-Partition value corresponding index data}

- `DELETE http://host:port/kylin/api/models/segments/multi_partition`

- URL Parameters
  
  - `project` - `required` `string`,project name.

  - `model` - `required` `string`,model name.
    
  - `segment_id` - `required` `string`, Segment Id.
  
  - `sub_partition_values` - `required` `string`, sub-partition values.
  
- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example
  
  ```sh
  curl -X DELETE \
    'http://host:port/kylin/api/models/segments/multi_partition?project=multi_partition_project&model=multi_partition_model&segment_id=ff839b0b-2c23-4420-b332-0df70e36c343&sub_partition_values=0,1' \
    -H 'accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
  ```
  
- Response Example

  ```json
  {
      "code": "000",
      "data": "",
      "msg": ""
  }
  ```
  
### Refresh the index data corresponding to the Sub-Partition value {#Refresh the index data corresponding to the Sub-Partition value}

- `PUT http://host:port/kylin/api/models/{model_name}/segments/multi_partition`

- URL Parameters
  
  - `model_name` - `required` `string`,model name.
  
- HTTP Body: JSON Object

  - `project` - `required` `string`,project name.
  
  - `segment_id` - `required` `string`,Segment id.
  
  - `sub_partition_values` - `required` `array`,sub-partition values.

  - `yarn_queue` - `optional` `string`, specify the YARN queue used by the job, it can be set after these two parameters were set: kylin.engine-yarn.queue.in.task.enabled (whether to allow set specified YARN queue for build task, default value is false), kylin.engine-yarn.queue.in.task.available (available YARN queues, separate them with English commas)

  - `tag` - `optional` `object`, job tag, if the field is set, when calling the [Get Job List](../job_api.md) API, the field will be the same back when returning the job. It can be used for system integration, mark the job and deal with it accordingly. By default, the maximum size of value is 1024 KB , which can  be set by the configure kylin.job.tag-max-size=1024.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example
  
  ```sh
  curl -X PUT \
    http://host:port/kylin/api/models/SSB_LINEORDER/segments/multi_partition \
    -H 'accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{"project":"default","segment_id":"60615c5e-2ae1-4ee9-b88d-f00f1f101a8a","sub_partition_values":[["1"], ["2"]]}'
  ```
  
- Response Example

  ```json
  {
      "code": "000",
      "data": {
          "jobs": [
              {
                  "job_name": "SUB_PARTITION_REFRESH",
                  "job_id": "4fa1a0a1-6a97-4be9-bdf6-8957c5f80114"
              }
          ]
      },
      "msg": ""
  }
  ```

### Get Segment List {#Get Segment List}
Please refer to [Get Segment List](../segment_managment_api.md) for multi-level partition model to get Segment list.

### Get Segment details {#Get Segment details}
- `GET http://host:port/kylin/api/models/{model_name}/segments/multi_partition`

- URL Parameters
  
  - `project` - `required` `string`, project name.
  
  - `segment_id` - `required` `string`, Segment Id.
  
  - `page_offset` - `optional` `int`, pagination page, the default value is 0.
  
  - `page_size` - `optional` `int`, page size, the default value is 10.

  - `model_name` - `required` `string`, model name.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example
  
  ```sh
  curl -X GET \
   'http://host:port/kylin/api/models/multi_partition_model/segments/multi_partition?project=multi_partition_project&segment_id=ffdfc037-7c63-4499-986d-9b680276161e' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Authorization: Basic QURNSU46a3lsaW5AMjAxOQ==' \
  -H 'Content-Type: application/json;charset=utf-8' \
  ```
  
- Response Example

  ```json
  {
      "code": "000",
      "data": {
          "value": [
              {
                  "id": 2,
                  "values": [
                      "3"
                  ],
                  "status": "ONLINE",
                  "last_modified_time": 1609213843048,
                  "source_count": 0,
                  "bytes_size": 3747
              },
              {
                  "id": 3,
                  "values": [
                      "4"
                  ],
                  "status": "ONLINE",
                  "last_modified_time": 1609213843048,
                  "source_count": 0,
                  "bytes_size": 3747
              }
          ],
          "offset": 0,
          "limit": 10,
          "total_size": 2
      },
      "msg": ""
  }
  ```
  
### Query mapping settings {#Query mapping settings}
- `PUT http://host:port/kylin/api/models/{model_name}/multi_partition/mapping`

- URL Parameters

  - `model_name` - `required` `string`,model name.

- HTTP Body: JSON Object
  
  - `project` - `required` `string`,project name.
  
  - `alias_columns` - `required`,`array<string>`, The corresponding column name that the sub-partition column needs to be mapped to
  
  - `multi_partition_columns` - `required`,`array<string>`, sub-partition column names
  
  - `value_mapping` - `required`,`array<object>`, partition value mapping
  
    - `origin` - `required`,`array<string>`, sub-partition column value
  
    - `target` - `required`, `array<string>`, sub-partition column value mapping value
    
- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example
  
  ```sh
  curl -X PUT \
    http://host:port/kylin/api/models/test_multi/multi_partition/mapping \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Authorization: Basic QURNSU46a3lsaW5AMjAxOQ==' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{
  	"project": "multi_partition_project",
      "alias_columns": ["KYLIN_SALES.LEAF_CATEG_ID"],
      "multi_partition_columns": ["KYLIN_SALES.LSTG_SITE_ID"],
      "value_mapping": [
        {
          "origin": ["Beijing"], 
          "target":["North"]
        },
        {
          "origin":["Shanghai"],
          "target":["South"]
        }
      ]
  }'
  ```
  
- Response Example

  ```json
  {
      "code": "000",
      "data": "",
      "msg": ""
  }
  ```
