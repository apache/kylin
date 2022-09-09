---
title: Model Build API
language: en
sidebar_label: Model Build API
pagination_label: Model Build API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - model build api
draft: false
last_update:
    date: 08/12/2022
---


> Reminders:
>
> 1. Please read [Access and Authentication REST API](../authentication.md) and understand how authentication works.
> 2. On Curl command line, don't forget to quote the URL if it contains the special char `&`.

### Load Segment{#Load-Segment-Expert}

- `POST http://host:port/kylin/api/models/{model_name}/segments`

- URL Parameters
  
  - `model_name` - `required` `string`, model name.
  
- HTTP Body: JSON Object
  - `project` - `required` `string`, project name.
  - `start` - `optional` `string`, when the model is built in full, it is not required, and it is required when the model is incrementally built. start time of segment (partition column exist), type: timestamp, unit: ms. For example, `694195200000` means `1992-01-01 00:00:00`.
  - `end` - `optional` `string`, when the model is built in full, it is not required, and it is required when the model is incrementally built. end time of segment (partition column exist), type: timestamp, unit: ms. For example, `883584000000` means `1998-01-01 00:00:00`.
  - `build_all_indexes` - `optional` `boolean`, build all indexes in the new segment, default value is `true`
  - `sub_partition_values` - `optional` `Array` , sub-partition value, used for multi-level partition model. the default is empty. For multi-leve partition model, when `build_all_indexes` is `true` (all indexes need to be built), this value is required. When `build_all_indexes` is `false`(when creating an empty segment), the value must be empty.
  - `priority` - `optional` `int`, set job priority with range `0-4` which indicates the priority from high to low. Default value is  `3` 
  - `build_all_sub_partitions` - `optional` `boolean`, build all sub partition values for multi-level partition model,default value is `false`
  - `yarn_queue` - `optional` `string`, specify the YARN queue used by the job, it can be set after these two parameters were set: kylin.engine-yarn.queue.in.task.enabled (whether to allow set specified YARN queue for build task, default value is false), kylin.engine-yarn.queue.in.task.available (available YARN queues, separate them with English commas)
  - `tag` - `optional` `object`, job tag, if the field is set, when calling the [Get Job List](../job_api.md) API, the field will be the same back when returning the job. It can be used for system integration, mark the job and deal with it accordingly. By default, the maximum size of value is 1024 KB , which can  be set by the configure kylin.job.tag-max-size=1024.
    
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X POST \
    'http://host:port/kylin/api/models/SSB_LINEORDER/segments' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{"project":"doc_expert", "start": "694195200000", "end": "883584000000","build_all_indexes":true,"sub_partition_values":[["1"],["2"]],"build_all_sub_partitions":false}'
  ```

- Response Example

  ```json
    {
        "code": "000",
        "data": {
            "jobs": [
                {
                    "job_name": "INC_BUILD",
                    "job_id": "61f4d697-e648-4ace-8e52-155829417b2a"
                },
                {
                    "job_name": "INDEX_BUILD",
                    "job_id": "0217b970-4525-468b-ba25-58bbb168d612"
                }
            ]
        },
        "msg": ""
    }
  ```


