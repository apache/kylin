---
title: Snapshot Management API
language: en
sidebar_label: Snapshot Management API
pagination_label: Snapshot Management API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - snapshot management api
draft: false
last_update:
    date: 08/12/2022
---

> Note:
>
> 1. Before reading, please finish the [Access and Authentication REST API](authentication.md) chapter to understand how authentication works.
> 2. On Curl command line, don't forget to quote the URL if it contains the special char `&`.


### <span id="switch">Snapshot Management</span>

> **Note**: When the Snapshot Management API is enabled, the system will no longer build or refresh snapshots automatically, please manually manage snapshots according to the snapshot list. 

- `PUT http://host:port/kylin/api/projects/{project}/snapshot_config`
- Request Permission: ADMIN permission and above
- Introduced in: 4.2.2
- URL Parameters
    - `project` - `required` `string`, project name.
- HTTP Body: JSON Object
    - `snapshot_manual_management_enabled` - `optional` `boolean`, whether to enable snapshot management. The default value is false.
- HTTP Header
    - `Accept: application/vnd.apache.kylin-v4-public+json`
    - `Accept-Language: en`
    - `Content-Type: application/json;charset=utf-8`
- Curl Request Example
```sh
curl -X PUT \
  'http://localhost:7070/kylin/api/projects/gc_test/snapshot_config' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{ "snapshot_manual_management_enabled": true }'
```


- Response
```json
{
    "code": "000", 
    "data": "", 
    "msg": ""
}
```

### <span id="create">Create Snapshots</span>

- `POST http://host:port/kylin/api/snapshots`
- Request Permission: OPERATION permission and above
- Introduced in: 4.2.2 (Partition building: since 4.2.6)
- HTTP Body: JSON Object
    - `project` - `required` `string`, project name.
    - `tables` - `optional` `array[string]`, load tables with the format `DB.TABLE`
    - `databases`  - `optional` `array[string]`, load all the tables under this database
       **Note:** The above two parameters `databases` and `tables` cannot be empty at the same time, which means you must use one of them to load tables.
    - `priority` - `optional` `integer`, set job priority with range `0-4` which indicates the priority from high to low. Default value is  `3` 
    - `options`   - `optional` `map[string:args]`, mapping from table (`DB.TABLE`) to argument set, `args` can be set as follows：
      -  `partition_col` -  `optional` `string` partition column of responding table, null by default.
      -  `partitions_to_build` - `optional` `string` only refresh the special partitions
    - `yarn_queue` - `optional` `string`, specify the YARN queue used by the job, it can be set after these two parameters were set: kylin.engine-yarn.queue.in.task.enabled (whether to allow set specified YARN queue for build task, default value is false), kylin.engine-yarn.queue.in.task.available (available YARN queues, separate them with English commas)
    - `tag` - `optional` `object`, job tag, if the field is set, when calling the [Get Job List](job_api.md) API, the field will be the same back when returning the job. It can be used for system integration, mark the job and deal with it accordingly. The maximum size of value is 1 KB.
    **Note**: If the loaded table already exists in the system, it will be reloaded.

- HTTP Header
    - `Accept: application/vnd.apache.kylin-v4-public+json`
    - `Accept-Language: en`
    - `Content-Type: application/json;charset=utf-8`
- Curl Request Example
```sh
curl -X POST \
  'http://localhost:7070/kylin/api/snapshots' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{"project":"gc_test",  "tables": ["SSB.P_LINEORDER",  "DEFAULT.TEST_KYLIN_FACT"], "options":{"DEFAULT.TEST_KYLIN_FACT":{"partition_col":"CAL_DT","incremental_build":"true","partitions_to_build":["2012-03-01","2012-03-04"]}}}'
```
- Curl Response Example
```json
{
    "code": "000", 
    "data": {
        "jobs": [{
            "job_name": "SNAPSHOT_BUILD", 
            "job_id": "65b3b0a4-d4d2-4a5b-af29-b190ca420543"
        },  {
            "job_name": "SNAPSHOT_BUILD", 
            "job_id": "24aafb93-1cde-43d1-a627-8cd592f51cfe"
        }]
    }, 
    "msg": ""
}
```


### <span id="config">Set Partition Column</span>


- `POST http://host:port/kylin/api/snapshots/config`
- Request Permission: OPERATION permission and above
- Introduced in: 4.2.6
- HTTP Body: JSON Object
  - `project` - `required` `string`, project name.
  - `table_partition_col` - `required`  `map[string:string]` The mapping from table (DB.TABLE) to chosen partition column.
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`
- Curl Request Example
```sh
 curl -X POST \
   'http://localhost:7070/kylin/api/snapshots/config' \
   -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
   -H 'Accept-Language: en' \
   -H 'Authorization: Basic QURNSU46S1lMSU4=' \
   -H 'Content-Type: application/json;charset=utf-8' \
   -d '{"project":"gc_test","table_partition_col":{"DEFAULT.TEST_KYLIN_FACT":"CAL_DT"}}'
```


- Response
```json
 {
     "code": "000",
     "data": "",
     "msg": ""
 }
```

### <span id="update">Refresh Snapshots</span>

- `PUT http://host:port/kylin/api/snapshots`

- Request Permission: OPERATION permission and above

- Introduced in: 4.2.2 (Partition building: since 4.2.6)

- HTTP Body: JSON Object
    - `project` - `required` `string`, project name.

    - `tables` - `optional` `array[string]`, load tables with the format `DB.TABLE`

    - `databases`  - `optional` `array[string]`, load all the tables under this database

      **Note:** The above two parameters `databases` and `tables` cannot be empty at the same time, which means you must use one of them to load tables.

    - `priority` - `optional` `int`, set job priority with range `0-4` which indicates the priority from high to low. Default value is  `3` 
    - `options`   - `required` `map[string:args]`, mapping from table (`DB.TABLE`) to arguments, `args` can be set as follows：
         -  `partition_col` -  `required` `string`, partition column of responding table, null by default. If you have  set partition columns for tables to be refreshed, here **need to enter the corresponding partition column**
         -   `incremental_build` - `optional` `boolean`, whether keep the built partitions, false by default.
         -   `partitions_to_build` - `optional` `array[string]`, only refresh the special partitions.
- `yarn_queue` - `optional` `string`, specify the YARN queue used by the job, it can be set after these two parameters were set: kylin.engine-yarn.queue.in.task.enabled (whether to allow set specified YARN queue for build task, default value is false), kylin.engine-yarn.queue.in.task.available (available YARN queues, separate them with English commas)
  
    - `tag` - `optional` `object`, job tag, if the field is set, when calling the [Get Job List](job_api.md) API, the field will be the same back when returning the job. It can be used for system integration, mark the job and deal with it accordingly. By default, the maximum size of value is 1024 KB , which can  be set by the configure kylin.job.tag-max-size=1024.

    **Note**: 
    
    - If the loaded table already exists in the system, it will be reloaded
    - The `partition_col` in the parameter `options` is temporarily **required**. If the interface for setting the partition column has been called before, the correct parameter value must also be filled here

- HTTP Header
    - `Accept: application/vnd.apache.kylin-v4-public+json`
    - `Accept-Language: en`
    - `Content-Type: application/json;charset=utf-8`
    
- Curl Request Example
```sh
curl -X PUT \
  'http://localhost:7070/kylin/api/snapshots' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{"project":"gc_test",  "tables": ["SSB.P_LINEORDER",  "DEFAULT.TEST_KYLIN_FACT"],"options":{"DEFAULT.TEST_KYLIN_FACT":{"partition_col":"CAL_DT","incremental_build":true,"partitions_to_build":["2012-03-01","2012-03-04"]}}}'
```
- Curl Response Example
```json
{
    "code": "000", 
    "data": {
        "jobs": [{
            "job_name": "SNAPSHOT_REFRESH", 
            "job_id": "65b3b0a4-d4d2-4a5b-af29-b190ca420543"
        },  {
            "job_name": "SNAPSHOT_REFRESH", 
            "job_id": "24aafb93-1cde-43d1-a627-8cd592f51cfe"
        }]
    }, 
    "msg": ""
}
```


### <span id="delete">Delete Snapshots</span>

- `DELETE http://host:port/kylin/api/snapshots`
- Request Permission: OPERATION permission and above
- Introduced in: 4.2.2
- URL Parameters
    - `project` - `required` `string`, project name.
    - `tables` - `required` `array[string]`, snapshot tables to be deleted, for example：DB.TABLE, multiple tables are splitted by comma. Because the http protocol has a limit on the size of the request header, it is recommended that the length of url is less than 100.
- HTTP Header
    - `Accept: application/vnd.apache.kylin-v4-public+json`
    - `Accept-Language: en`
    - `Content-Type: application/json;charset=utf-8`
- Curl Request Example
```sh
curl -X DELETE \
  'http://localhost:7070/kylin/api/snapshots?project=gc_test&tables=SSB.P_LINEORDER%2CDEFAULT.TEST_KYLIN_FACT' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8'
```


- Response Example
```json
{
    "code":"000", 
    "data": {
        "affected_jobs":[
            {
                "database": "DEFAULT", 
                "table": "KYLIN_CAL", 
                "job_id": "e3aa809b-5e73-42a5-a1e1-649d53b16e2c"
            }, 
            {
                "database": "DEFAULT", 
                "table": "P_LINEORDER", 
                "job_id": "e3aa809b-5e73-42a5-a1e1-649d53b16e2b"
            }
        ]
    }, 
    "msg":""
}
```


### <span id="read">Get Snapshot List</span>

- `GET http://host:port/kylin/api/snapshots`
- Request Permission: QUERY permission and above
- Introduced in: 4.2.2
- URL Parameters
    - `project` - `required` `string`, project name.
    - `table` - `optional` `string`, search key word. Default value is an empty string, will display all the snapshots.
    - `page_offset` -`optional`  `int`, offset of returned result, `0` by default.
    - `page_size` -`optional`  `int`, quantity of returned result per page, `10` by default.    
- HTTP Header
    - `Accept: application/vnd.apache.kylin-v4-public+json`
    - `Accept-Language: en`
    - `Content-Type: application/json;charset=utf-8`


- Curl Request Example
```sh
curl -X GET \
  'http://localhost:7070/kylin/api/snapshots?project=gc_test' \
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
                "table":"P_LINEORDER", 
                "database":"SSB", 
                "usage": 5, 
                "storage": 8555, 
                "fact_table_count": 2
                "lookup_table_count": 2, 
                "last_modified_time": 1602315332279, 
                "status": "REFRESHING"            
            }, 
            Object{...}, 
            Object{...}, 
            Object{...}
        ], 
        "offset":0, 
        "limit":10, 
        "total_size":4
    }, 
    "msg":""
}
```
