---
title: Use callback API to monitor job status
language: en
sidebar_label: Use callback API to monitor job status
pagination_label: Use callback API to monitor job status
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - callback api
    - monitor job
draft: false 
last_update: 
    date: 08/12/2022
---

The success returned by calling the required job API only indicates the job is successfully started. Kylin supports callback API to confirm the job execution state, it will return success if the job is successfully completed; it will return error if the job fails and also the error code to help troubleshooting. 

### How to use 

Deploy an online service that accepts POST requests and add the configuration parameter `kylin.job.finished-notifier-url` to kylin.properties with the service URL as its parameter value.

### POST message example 

Suppose the value of parameter `kylin.job.finished-notifier-url` is set to `http://127.0.0.1:7777`. When Kylin finishes the job, it will send the following message to `http://127.0.0.1:7777`. 

**Header**

```sh
Content-Type: application/json
Content-Length: 262
Host: 127.0.0.1:7777
Connection: Keep-Alive
User-Agent: Apache-HttpClient/4.5.3 (Java/1.8.0_171)
Accept-Encoding: gzip,deflate
```

**Body**

```sh
{
"job_id": "6044ac09-27bc-4968-a6b0-881c4c0abf89",
"project": "test",
"model_id": "91402da4-b991-4cd8-b164-7f62b9e91b69",
"segment_ids": [
  "9e5a89b9-00f1-4296-adc4-139113030e83",
  "f26dcbc9-b786-4112-a4f6-94280bf10265",
  "f3dc9864-0e1a-417b-a46d-3255e8cb6473"
],
"index_ids": [
  20001,
  10001
],
"duration": 27699,
"job_state": "ERROR",
"job_type": "INDEX_BUILD",
"segment_time_range": [
  {
    "segment_id": "9e5a89b9-00f1-4296-adc4-139113030e83",
    "data_range_start": 1338480000000,
    "data_range_end": 1354291200000
  },
  {
    "segment_id": "f26dcbc9-b786-4112-a4f6-94280bf10265",
    "data_range_start": 1325347200000,
    "data_range_end": 1338480000000
  },
  {
    "segment_id": "f3dc9864-0e1a-417b-a46d-3255e8cb6473",
    "data_range_start": 1354291200000,
    "data_range_end": 1370016000000
  }
],
"segment_partition_info": [{
      "segment_id": "9e5a89b9-00f1-4296-adc4-139113030e83",
      "partition_info": [{
          "id": 0,
          "values": ["674"],
          "status": "ONLINE",
          "last_modified_time": 1623390508821,
          "source_count": 34,
          "bytes_size": 1772
      }, {
          "id": 1,
          "values": ["22"],
          "status": "ONLINE",
          "last_modified_time": 1623390508821,
          "source_count": 30,
          "bytes_size": 1800
      }]
  }],
  "snapshot_job_info":null,
  "err_code":"KE-060100201",
  "msg":"KE-060100201: An Exception occurred outside Kylin.",
  "suggestion":"Please check whether the external environment(other systems, components, etc.) is normal.",
  "start_time":1656604800000,
  "end_time":1656608400000,
  "tag":null,
  "code":"999",
  "stacktrace":"KE-060100201: An Exception occurred outside Kylin. org.apache...."
}
```

### Callback API parameters 

- HTTP method: POST
- Content-Type: application/json
- URL Parameters
  - `project` - `string`, project name
  - `job_id - String`, job ID  
  - `model_id` - `string`, model UUID 
  - `segment_ids` - `list<string>`, segment UUID
  - `index_ids` - `list<string>`, index UUID
  - `duration` - `long`, build duration in milliseconds
  - `job_state` - `string`, job state when the job ends, value: `SUCCEED` if the job succeeds, `DISCARDED` if the job is discarded, `ERROR` if the job fails, or `PAUSED` if the job is paused 
  - `job_type` - `string`，job type，including：`INDEX_BUILD`、`INDEX_REFRESH`、`INDEX_MERGE`、`INC_BUILD`、`SUB_PARTITION_BUILD`、`SUB_PARTITION_REFRESH`、`SNAPSHOT_BUILD`、`SNAPSHOT_REFRESH`
  - `segment_time_range` - `list`，segment time range
    - `segment_id` - `string`，segment UUID
    - `data_range_start` - `long`, timestamp in milliseconds, start time of segment build
    - `date_range_end` - `long`, timestamp in milliseconds, end time of segment build
  - `segment_partition_info` - `list`, information about segment under partition including:  
    - `segment_id`:  ` string`, segment UUID
    - `partition_info`: `list`, partition information 
      - `id`: `long`, partition ID
      - `values`: `list<long>`, partition values
      - `status`: `string`, partition status, value:`ONLINE` if the partition is online,`LOADING` if the partition is being built, or `REFRESHING` if the partition is being refreshed
      - `last_modified_time`: `long`, timestamp in milliseconds, last modified time
      - `source_count`: `long`, number of rows 
      - `bytes_size`:  `long`, storage capacity
  - `snapshot_job_info` - `object`，information about snapshot job, including:
    - `table`：`string`，table name of the snapshot
    - `database`：`string`，database name of the snapshot
    - `total_rows`：`long`，total rows of the snapshot
    - `storage`： `long`，disk space of the snapshot
    - `last_modified_time`：timestamp, last time the snapshot was built
    - `status`：status of the snapshot, value: `ONLINE` and `OFFLINE`
    - `select_partition_col`：partition column of the snapshot, it takes effect only for incremental snapshots and `null` for full snapshots
  - `msg` - `string` , job error message
  - `error_code` - `string` , error code in Kylin
  - `suggestion` - `string`, suggestion on how to solve the issue
  - `start_time` - long, timestamp in milliseconds, the start time of the task
  - `end_time` - long, timestamp in milliseconds, the end time of the task
  - `tag` - string，customized tag, used for system integration
  - `code` -  string，status code，value: `null` if job succeeds, `401` if unauthorized, `999` other unrecognized status
  - `stacktrace` - string，stack information of exceptions

### Known limitations 

- For a job in PAUSED or ERROR state, if the job is canceled directly, the job state will be changed to DISCARDED, but the job state change will not be reported via the callback API. 

- If a job in PAUSED or ERROR state is rerun and then canceled when running, the job state change will be reported via the callback API. 

