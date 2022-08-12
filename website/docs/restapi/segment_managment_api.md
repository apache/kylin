---
title: Segment Management API
language: en
sidebar_label: Segment Management API
pagination_label: Segment Management API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - segment management api
draft: false
last_update:
    date: 12/08/2022
---

> Reminders:
>
> 1. Please read [Access and Authentication REST API](authentication.md) and understand how authentication works.
> 2. On Curl command line, don't forget to quote the URL if it contains the special char `&`.



### Get Segment List {#Get-Segment-List}

> Call this API to acquire the Segment list of certain model. After calling the "Replenish Indexes among Segments" API, use this API to confirm if segments are successfully built.  

- `GET http://host:port/kylin/api/models/{model_name}/segments`

- URL Parameters
  - `model_name` - `required` `string`, model name.
  - `project` - `required` `string`, project name.
  - `page_offset` - `optional` `int`, offset of returned result, `0` by default.
  - `page_size` - `optional` `int`, quantity of returned result per page, `10` by default.
  - `start` - `optional` `string`, start time of segments, `1` by default, type: timestamp, unit: ms.
  - `end` - `optional` `string` , end time of segments, `9223372036854775806` by default, type: timestamp, unit: ms.

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://localhost:7070/kylin/api/models/SSB_LINEORDER/segments?project=doc_expert' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Example
  
  ```json
  {
      "code": "000",
      "data": {
          "value": [
              {
                  "id": "99c547b7-1bc1-4b57-b62a-127c080e1fd2",
                  "name": "20120101000000_20130101000000",
                  "create_time_utc": 1609209524110,
                  "status": "READY",
                  "segRange": {
                      "@class": "org.apache.kylin.metadata.model.SegmentRange$TimePartitionedSegmentRange",
                      "date_range_start": 1325347200000,
                      "date_range_end": 1356969600000
                  },
                  "timeRange": null,
                  "parameters": null,
                  "dictionaries": null,
                  "snapshots": null,
                  "last_build_time": 1609209642839,
                  "source_count": 2,
                  "source_bytes_size": 798009,
                  "column_source_bytes": {},
                  "ori_snapshot_size": {},
                  "additionalInfo": {
                      "segment_path": "hdfs://nameservice1/kylin/jrc_kylin/multi_partition/parquet/d5b94380-f84f-481f-b4bb-ffa3f6b3b391/99c547b7-1bc1-4b57-b62a-127c080e1fd2",
                      "file_count": "14"
                  },
                  "is_encoding_data_skew": false,
                  "is_snapshot_ready": false,
                  "is_dict_ready": false,
                  "is_flat_table_ready": false,
                  "is_fact_view_ready": false,
                  "multi_partitions": [],
                  "max_bucket_id": 13,
                  "bytes_size": 12520,
                  "hit_count": 0,
                  "status_to_display": "ONLINE",
                  "index_count": 7,
                  "index_count_total": 7,
                  "multi_partition_count": 2,
                  "multi_partition_count_total": 2,
                  "row_count": 13,
                  "second_storage_nodes":[
                      {
                          "name":"sandbox.hortonworks.com",
                          "ip":"10.1.2.55",
                          "port":9500
                      }
                  ],
                  "second_storage_size":12989,
                  "has_base_table_index":true,
                  "has_base_agg_index":true,
                  "has_base_table_index_data":true,
                  "last_modified_time": 1609209642839
              }
          ],
          "offset": 0,
          "limit": 10,
          "total_size": 1
      },
      "msg": ""
  }
  ```




### Refresh/Merge Segments{#Refresh-Segments-Expert}

> Call this API to refresh segments or merge consecutive segments. Use this API to refresh Segment when there are index changes, or to merge multiple consecutive small segments to control file number and improve query performance. 

- `PUT http://host:port/kylin/api/models/{model_name}/segments`

- URL Parameters
  
  - `model_name` - `required` `string`, model name.
  
- HTTP Body: JSON Object
  
  - `project` - `required` `string`, project name.
  - `type` - `optional` `string`, refresh segments or merge continuous segments, the optional value should be either `REFRESH` or ` MERGE` and the default value is `REFRESH`.
  - `ids` - `optional` `array[string]`, segment id list.
  - `names` - `optional` `array[string]`, segment name list.
  > **Notice:** For `ids` and `names`, one of them must be set, and both cannot be set at the same time or both are empty at the same time.
  >
  > - `priority` - `optional` `int`, set job priority with range `0-4` which indicates the priority from high to low. Default value is  `3` 
  - `yarn_queue` - `optional` `string`, specify the YARN queue used by the job, it can be set after these two parameters were set: kylin.engine-yarn.queue.in.task.enabled (whether to allow set specified YARN queue for build task, default value is false), kylin.engine-yarn.queue.in.task.available (available YARN queues, separate them with English commas)
- `tag` - `optional` `object`, job tag, if the field is set, when calling the [Get Job List](job_api.md) API, the field will be the same back when returning the job. It can be used for system integration, mark the job and deal with it accordingly. By default, the maximum size of value is 1024 KB , which can  be set by the configure kylin.job.tag-max-size=1024.
  
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X PUT \
    'http://localhost:7070/kylin/api/models/SSB_LINEORDER/segments' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{"project":"doc_expert", "ids":["8f3b4040-aa75-4e74-9730-6bf1cae61745"], "type":"REFRESH"}'
  ```

- Response Example

  ```json
    {
        "code":"000",
        "data":"",
        "msg":""
    }
  ```
  
  
  
### Delete Segments{#Delete-Segments-Expert}

- `DELETE http://host:port/kylin/api/models/{model_name}/segments`

- URL Parameters
  - `model_name` - `required` `string`, model name.
  - `project` - `required` `string`, project name.
  - `purge` - `required` `boolean`, whether purge all segments or not.
  - `ids` - `optional` `array[string]`, segment id list. 
  - `names` - `optional` `array[string]`, segment name list.
  - `force` - `optional` `boolean`, whether force to delete, default value is "false".
> **Reminders**： 
>
> 1.if `purge` is `false`, for `ids` and `names`, one of them must be set, and both cannot be set at the same time or both are empty at the same time.
>
> 2.Because the http protocol has a limit on the size of the request header, it is recommended that the count of the `ids` or `names` is less than 100.
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X DELETE \
    'http://localhost:7070/kylin/api/models/SSB_LINEORDER/segments?project=doc_expert&ids=291b9926-eaba-42d1-9d70-0a587992bea7&purge=false' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Example

  ```json
    {
        "code":"000",
        "data":"",
        "msg":""
    }
  ```



### Check Segment {#Check_Segment}

> Verify whether there are overlapping segments within the model 
>

- `POST http://host:port/kylin/api/models/{model_name}/segments/check`

- Request Permission: Operation permission and above

- Effective Versions: 4.1.1 and above

- URL Parameters

  - `model_name` - `required` `string`, model name.

- HTTP Body: JSON Object

  - `project` - `required` `string`, project name.
  - `start` - `required` `string`, start time, type: timestamp, unit: ms.
  - `end` - `required` `string`, end time, type: timestamp, unit: ms.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X POST \
    'http://localhost:7070/kylin/api/models/ssb_model/segments/check' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{
        	"project":"ssb",
        	"start":"775785600000",
        	"end":"775789200000"
        }'
  ```

- Response Example

  ```json
  {
      "code": "000",
      "data": {
          "segments_overlap": [
              {
                  "segment_id": "17df7def-f06b-4b67-81d8-4c8368e714dc",
                  "segment_name": "19920101000000_19980802000000"
              },
              {
                  "segment_id": "1a9c070b-3847-48c6-b938-7109379eef9b",
                  "segment_name": "19980802000000_19980803000000"
              },
              {
                  "segment_id": "5cece637-daef-47f5-88e9-85b6a247d357",
                  "segment_name": "19980803000000_19980804000000"
              }
          ]
      },
      "msg": ""
  }
  ```



### Replenish Indexes of All Segments {#Replenish-Indexes-All-Segments}

> Note：Specify the model, build all the missing indexes for all Segments of the model

- `POST http://host:port/kylin/api/models/{model_name}/indexes`

- URL Parameters

  - `model_name` - `required` `string`, model name.

- HTTP Body: JSON Object

  - `project` - `required` `string`, project name.
  - `priority` - `optional` `int`, set job priority with range `0-4` which indicates the priority from high to low. Default value is  `3`
  - `yarn_queue` - `optional` `string`, specify the YARN queue used by the job, it can be set after these two parameters were set: kylin.engine-yarn.queue.in.task.enabled (whether to allow set specified YARN queue for build task, default value is false), kylin.engine-yarn.queue.in.task.available (available YARN queues, separate them with English commas)
  - `tag` - `optional` `object`, job tag, if the field is set, when calling the [Get Job List](job_api.md) API, the field will be the same back when returning the job. It can be used for system integration, mark the job and deal with it accordingly. By default, the maximum size of value is 1024 KB , which can  be set by the configure kylin.job.tag-max-size=1024.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X POST \
    'http://localhost:7070/kylin/api/models/ssb_test/indexes' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{"project":"ssb"}'
  ```

- Response Details

  - `type`, job type
    - NORM_BUILD, Build normally
    - NO_LAYOUT, All indexes to be completed are not online
    - NO_SEGMENT, All segments to be completed are not online
  - `job_id`, the job id will be returned if the type is NORM_BUILD. Otherwise this value will be empty if there is no index or segment.

- Response Example

  ```json
  {
      "code":"000",
      "data":{
          "type":"NORM_BUILD",
          "job_id":"e3aa809b-5e73-42a5-a1e1-649d53b16e2c"
      },
      "msg":""
  }
  ```



### Replenish Indexes among Segments{#Replenish-Indexes-among-Segments}

- `POST http://host:port/kylin/api/models/{model_name}/segments/completion`
- URL Parameters
    - `model_name` - `required` `string`, model name
    - `project` - `required` `string`, project name
    - `parallel` - `optional` `boolean`, whether build with parallel tasks, by default it is false
    - `ids` - `optional` `array[string]`, segment id list
    - `names` - `optional` `array[string]`, segment name list
    > **Notice:** For `ids` and `names`, one of them must be set, and both cannot be set at the same time or both are empty at the same time.
    - `yarn_queue` - `optional` `string`, specify the YARN queue used by the job, it can be set after these two parameters were set: kylin.engine-yarn.queue.in.task.enabled (whether to allow set specified YARN queue for build task, default value is false), kylin.engine-yarn.queue.in.task.available (available YARN queues, separate them with English commas)
    - `tag` - `optional` `object`, job tag, if the field is set, when calling the [Get Job List](job_api.md) API, the field will be the same back when returning the job. It can be used for system integration, mark the job and deal with it accordingly. By default, the maximum size of value is 1024 KB , which can  be set by the configure kylin.job.tag-max-size=1024.


- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`
  
- Curl Request Example

  ```sh
  curl -X POST \
  'http://localhost:7070/kylin/api/models/m1/segments/completion?project=ssb&names=19900101000000_19950101000000,19950101000000_19970101000000' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Example

  ```json
    {
        "code": "000",
        "data": {
            "jobs": [
                {
                    "job_name": "INDEX_BUILD",
                    "job_id": "74e28420-e317-42a9-a221-a7b381b5aeea"
                }
            ],
            "failed_segments": []
        },
        "msg": ""
    }
  ```
