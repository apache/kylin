---
title: Job API
language: en
sidebar_label: Job API
pagination_label: Job API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - job api
draft: false
last_update:
    date: 08/12/2022
---

> Reminders:
>
> 1. Please read [Access and Authentication REST API](authentication.md) and understand how authentication works.
> 2. On Curl command line, don't forget to quote the URL if it contains the special char `&`.



* [Get Job List](#Get-Job-List)
* [Operate Job](#operate-job)



### Get Job List {#Get-Job-List}

- `GET http://host:port/kylin/api/jobs`

- URL Parameters
  - `time_filter` - `required` `int`

    ![](image/job_api.png)

  - `project` - `optional` `string`, project name
  
  - `statuses` - `optional` `string`，job status，Optional values：`PENDING`,`RUNNING`,`FINISHED`,`ERROR`,`DISCARDED`,`STOPPED`,Separate multiple values with commas

  - `page_offset` - `optional` `int`, offset of returned result, 0 by default

  - `page_size` - `optional` `int`, quantity of returned result per page, 10 by default

  - `sort_by` -  `optional`  `string`, sort field, optional values：`last_modified` by default, `project id`,`job_name`,`target_subject`,`create_time`,`total_duration`

  - `reverse` - `optional` `boolean`, whether sort reverse, "true" by default
  
  - `key` - `optional` `string`, filter field, support job id or object name
  
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/jobs?time_filter=0&page_size=1' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```


- Response Example

  ```json
  {
      "code":"000",
      "data":{
          "value":[
              {
                  "id":"232bf69f-6cdf-4dcc-a3aa-b6ca7651e98c",
                  "last_modified":0,
                  "duration":91020,
                  "exec_start_time":1577764731199,
                  "steps":null,
                  "job_status":"FINISHED",
                  "job_name":"INDEX_BUILD",
                  "data_range_start":0,
                  "data_range_end":9223372036854775807,
                  "target_model":"5b54898a-dd75-4146-abbe-de77c0cf77fb",
                  "target_segments":[
                      "3344c9bb-83fa-4128-803b-e18f27b0ccf8"
                  ],
                  "step_ratio":1,
                  "create_time":1577764730069,
                  "wait_time":1130,
                  "target_subject":"AUTO_MODEL_KYLIN_ACCOUNT_1",
                  "target_subject_error":false,
                  "project":"test",
                  "submitter":"ADMIN",
                  "exec_end_time":1577764822219,
                  "tag":"mark"
              }
          ],
          "offset":0,
          "limit":1,
          "total_size":1364
      },
      "msg":""
  }
  ```


### Operate Job {#operate-job}

- `PUT http://host:port/kylin/api/jobs/status`

- URL Parameters
  - `action` - `required` `string`, action types for jobs. Optional values are below:
    - `RESUME`, resume selected jobs from paused/error status
    - `DISCARD`, discard selected jobs
    - `PAUSE`, pause selected jobs
    - `RESTART`, restart selected jobs

  - `project` - `optional` `string`, project name. If only `project` is defined, it will operate all jobs under this project. Note: `project` and `job_ids` cannot be empty at the same time.

  - `job_ids` - `optional` `array<string>`, job id. If only `job_ids` is defined, it will operate all jobs with those ids. Note: `project` and `job_ids` cannot be empty at the same time.

  - - `statuses` - `optional` `array<string>`, filter jobs by statuses based on the filtering results of `project` and `job_ids`.
      - `PENDING`, pending jobs
      - `RUNNING`, running jobs
      - `FINISHED`, finished jobs
      -  `ERROR`, error jobs
      - `DISCARDED`, discarded jobs

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X PUT \
    'http://host:port/kylin/api/jobs/status' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{
           "action" : "PAUSE",
           "job_ids" : [
              "d7e4a098-10b6-4961-85b4-9eebfe29eb25",
              "80f4d168-1074-4218-875c-4c70a4334029"
           ],
           "project" : "ssb"
        }'
  ```


- Response Example

  ```json
  {
      "code":"000",
      "data":"",
      "msg":""
  }
  ```
