---
title: Data Source API
language: en
sidebar_label: Data Source API
pagination_label: Data Source API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - data source api
draft: false
last_update:
    date: 08/12/2022
---

> Reminder:
>
> 1. Please read [Access and Authentication REST API](authentication.md) and understand how authentication works.
>
> 2. On Curl command line, don't forget to quote the URL if it contains the special char `&`.


### Load Hive Table  {#Load-Hive-Table}

> Call this API to load Hive table metadata to Kylin. By default, when a new Hive table is added, the table metadata will not be loaded to Kylin. 

- `POST http://host:port/kylin/api/tables`

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object
  - `project` - `required` `string`, project name
  
  - `need_sampling` - `required` `boolean`, whether to enable table sampling
  
  - `sampling_rows` - `optional` `integer`, indicates the max number of sampling rows and the range is [10,000 - 20,000,000] .

    > Note: if you enable need_sampling, this parameter will be required.
  
  - `databases` - `optional` `[string]`, load all the tables under this database
  
  - `tables` - `optional` `[string]`, load tables with the format `DB.TABLE`
  
    **Note:**
  
    * If the loaded table already exists in the system, it will be reloaded.
  
    * The above two parameters `databases` and `tables` cannot be empty at the same time, which means you must use one of them to load tables.
  
- Curl Request Example

  ```sh
  curl -X POST \
    'http://host:port/kylin/api/tables' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{"project":"ssb","tables":["SSB.LINEORDER"],"need_sampling":false}'
  ```
  
- Response Details

  - `loaded`, successfully loaded tables
  - `failed`, failed to load tables
  
- Response Example

  ```json
  {
      "code": "000",
      "data": {
        			"loaded":["SSB.LINEORDER"],
        			"failed":[]
              },
      "msg": ""
  }
  ```



### Prepare Reload Hive Table   {#Prepare-Reload-Hive-Table}

> Call this API to compare the Hive table metadata in Kylin and that in the data source. For Hive table already loaded to Kylin and already used in model and index building, if some columns are deleted, Kyligence will return a failure when reading these columns. Use this API to find the metadata differences and evaluate whether to update the metadata in Kylin by reloading the Hive table. 

- `GET http://host:port/kylin/api/tables/pre_reload`

- Introduced in: 4.1.9

- URL Parameters

  - `project` - `required` `string`，project name

  - `table` - `required` `string`， reload table with the format `DB.TABLE`

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/tables/pre_reload?project=ssb&table=SSB.LINEORDER' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Details

- `has_datasource_changed`, source table structure has changed
  - `has_effected_jobs`, has unfinished jobs related to the table
  - `has_duplicated_columns`, has duplicated columns
  - `add_column_count`, number of new columns
  - `remove_column_count`, number of reduce columns
  - `data_type_change_column_count`, number of column type changes
  - `broken_model_count`, number of broken models
  - `remove_measures_count`, number of impact measures
  - `remove_dimensions_count`, number of dimensions affected
  - `remove_layouts_count`, number of deleted indexes
  - `add_layouts_count`, increased number of indexes
  - `refresh_layouts_count`, number of indexes refreshed
  - `snapshot_deleted`, snapshot is deleted
  - `duplicated_columns`, duplicate column whose format is database.table.column
  - `effected_jobs`，effected Job ID

- Response Example

  ```json
  {
      "code": "000",
      "data": {
        "has_datasource_changed": false,
        "has_effected_jobs": true,
        "has_duplicated_columns": true,
        "add_column_count": 0,
        "remove_column_count": 0,
        "data_type_change_column_count": 0,
        "broken_model_count": 0,
        "remove_measures_count": 0,
        "remove_dimensions_count": 0,
        "remove_layouts_count": 0,
        "add_layouts_count": 0,
        "refresh_layouts_count": 0,
        "snapshot_deleted": true,
        "dumplicated_columns": ["SSB.LINEORDER.PROFIT", "SSB.LINEORDER.LO_DISCOUNT"],
        "effected_jobs": ["266c9086-7ffe-44a1-9d5e-f9f9941b891d", "f42e5dd3-78e6-43f8-9bcb-edcb2c09312d"] 
      },
      "msg": ""
  }
  ```



### Reload Hive Table   {#Reload-Hive-Table}


- `POST http://host:port/kylin/api/tables/reload`

- Request Permission: MANAGEMENT permission and above

- Introduced in: 4.2.0

- HTTP Body: JSON Object

  - `project` - `required` `string`, project name

  - `table` - `required` `string`, specify the table, format：DB.TABLE

  - `need_sampling` - `required` `boolean`, whether to enable table sampling

  - `sampling_rows` - `optional` `integer`, indicates the max number of sampling rows and the range is [10,000 - 20,000,000] 

    > Note: if you enable need_sampling, this parameter will be required

  - `need_building` - `optional` `boolean`, whether to build a new index, `true` means to build, `false` means not to build, default value is `false`

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X POST \
    'http://host:port/kylin/api/tables/reload' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{"project":"ssb","table":"SSB.LINEORDER","need_sampling":false,"need_building":false}'
  ```

- Response Field

  - `sampling_id`, ids of table sampling jobs
  - `job_ids`, ids of the building jobs

- Response Example

  ```json
  {
      "code": "000",
      "data": {
  		"sampling_id":"",
  		"job_ids":["1234","1234"]
      },
      "msg": ""
  }
  ```



### Prepare Unload Table   {#Prepare-Unload-Table}

> Call this API to evaluate the risks of unloading Hive table metadata. There are cases where you need to offline some Hive tables from Kylin. Use this API to evaluate the impact of unloading Hive table metadata on related Kyligence models and jobs.

- `GET http://host:port/kylin/api/tables/{database}/{table}/prepare_unload`

- URL Parameters

  - `database` - `required` `string`，database name of the table to be deleted

  - `table` - `required` `string`，table name to be deleted

  - `project` - `required` `string`，project name

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/tables/SSB/LINEORDER/prepare_unload?project=ssb' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Field

  - `has_job`, whether there are running jobs of sampling or building snapshot in the current table
  - `has_model`, is the current table used by the model
  - `has_snapshot`, does the current table have a snapshot
  - `storage_size`, storage size of the current table snapshot (Byte)
  - `models`, model list

- Response Example

  ```json
  {
      "code": "000",
      "data": {
          "has_job": false,
          "has_model": true,
          "has_snapshot": true,
          "storage_size": 16616,
          "models": [
              "model"
          ]
      },
      "msg": ""
  }
  ```
  



### Unload Table   {#Unload-Table}

> Call this API to unload Hive table metadata from Kylin. After the API call, Hive table metadata will be unloaded from Kylin, and Kyligence can no longer read the table data, or update the index data related to the table. It's recommended calling the "Prepare Unload Table" API before calling this API.  

- `DELETE http://host:port/kylin/api/tables/{database}/{table}`

- URL Parameters

  - `database` - `required` `string`, database name of the table to be deleted

  - `table` - `required` `string`, table name to be deleted

  - `project` - `required` `string`, project name

  - `cascade` - `optional` `boolean`, delete all. default value is `false`
    - true:  Delete this source table with the snapshot, attached Kafka/Hive table, the referenced models, and stop/delete related jobs.
    - false: Only delete this source table with the snapshot, and stop related jobs. While the referenced models will be kept (BROKEN, can be fixed by reloading the table).

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X DELETE \
    'http://host:port/kylin/api/tables/SSB/LINEORDER?project=ssb' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Field

  - `date`, deleted table name


- Response Example

  ```json
  {
      "code": "000",
      "data": "SSB.LINEORDER",
      "msg": ""
  }
  ```



### Table Sampling  {#Sampling-Table}

> Call this API to enable data sampling to reflect the characteristics of Hive table data.

- `POST http://host:port/kylin/api/tables/sampling_jobs`

- Request Permission: MANAGEMENT permission and above

- Introduced in: 4.2.0

- HTTP Body: JSON Object

  - `project` - `required` `string`, project name
  - `qualified_table_name` - `required` `string`,specify the table,format:DB.TABLE
  - `rows` - `required` `integer`, indicates the max number of sampling rows and the range is [10,000 - 20,000,000] 
  - `priority` - `optional` `integer`, set job priority with range `0-4` which indicates the priority from high to low. Default value is  `3` 
  - `yarn_queue` - `optional` `string`, specify the YARN queue used by the job, it can be set after these two parameters were set: kylin.engine-yarn.queue.in.task.enabled (whether to allow set specified YARN queue for build task, default value is false), kylin.engine-yarn.queue.in.task.available (available YARN queues, separate them with English commas)
  - `tag` - `optional` `object`, job tag, if the field is set, when calling the [Get Job List](job_api.md) API, the field will be the same back when returning the job. It can be used for system integration, mark the job and deal with it accordingly. By default, the maximum size of value is 1024 KB , which can  be set by the configure kylin.job.tag-max-size=1024.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X POST \
    'http://host:port/kylin/api/tables/sampling_jobs' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{"project":"ssb","qualified_table_name":"SSB.LINEORDER","rows":20000,"priority":0}'
  ```

- Response Example

  ```json
  {
      "code":"000",
      "data":"",
      "msg":""
  }
  ```



### Get Partition Format of A Column {#Get-Partition-Column-Format} 

> When a column is used as a partition column in a model in Kylin, get the partition format of the column.

- `GET http://host:port/kylin/api/tables/column_format`

- Request Permission: Operation permission and above

- Introduced in: 4.2.0

- Request Parameters

  - `project` - `required` `string`, project name
  - `table` - `required` `string`, table name, format as DB.TABLE
  - `column_name` - `required` `string`, column name

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/tables/column_format?project=test&table=DEFAULT.KYLIN_SALES&column_name=PART_DT' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Field

  - `column_name` , column name
  - `column_format` , column format

- Response Example

  ```json
  {
      "code": "000",
      "data": {
            "partition_column": "PART_DT",
        		"format": "yyyy-MM-dd"
      },
      "msg": ""
  }
  ```



### Get Table Information{#Get-Table-Information}

> Call this API to get the metadata of a specified Hive table.

- `GET http://host:port/kylin/api/tables`

- Request Permission: READ permission and above.

- Introduced in: 4.2.0

- Request Parameters

  - `project` - `required` `string`, project name
  - `database` - `optional` `string`, database name, case sensitive
  - `table` - `optional` `string`, table name, case sensitive
  - `is_fuzzy` - `optional` `boolean`, whether to enable fuzzy matching for table names, `true` means to enable, `false` means to close, default value is `false`
  - `ext`  - `optional` `boolean`, specify whether the table's extension information is returned, `true` means to enable, `false` means to close, default value is `true`
  - `page_offset` - `optional` `int`, offset of returned result, `0` by default
  - `page_size` - `optional` `int`, quantity of returned result per page, `10` by default

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://localhost:7070/kylin/api/tables?project=test&database=SSB&table=KYLIN_SALES' \
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
                  "uuid": "6e638305-1a44-42dc-a161-5e06338dcb14",
                  "last_modified": 1600335525521,
                  "create_time": 1600335525522,
                  "version": "4.0.0.0",
                  "mvcc": 0,
                  "name": "KYLIN_SALES",
                  "columns": [
                      {
                          "id": "1",
                          "name": "TRANS_ID",
                          "datatype": "bigint",
                          "cardinality": null,
                          "min_value": null,
                          "max_value": null,
                          "null_count": null
                      }
                  ],
                  "source_type": 9,
                  "kafka_bootstrap_servers": null,
                  "subscribe": null,
                  "starting_offsets": null,
                  "table_type": "MANAGED",
                  "top": false,
                  "increment_loading": false,
                  "last_snapshot_path": null,
                  "database": "DEFAULT",
                  "exd": {
                      "owner": "root",
                      "create_time": "1524213799000",
                      "total_file_size": "0",
                      "hive_inputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                      "hive_outputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                      "location": "hdfs://sandbox.hortonworks.com:8020/apps/hive/warehouse/kylin_sales",
                      "partition_column": "",
                      "total_file_number": "0",
                      "last_access_time": "0"
                  },
                  "root_fact": false,
                  "lookup": false,
                  "primary_key": [],
                  "foreign_key": [],
                  "partitioned_column": null,
                  "partitioned_column_format": null,
                  "segment_range": null,
                  "storage_size": -1,
                  "total_records": 0,
                  "sampling_rows": [],
                  "last_build_job_id": null
              }
          ],
          "offset": 0,
          "limit": 10,
          "total_size": 3
      },
      "msg": ""
  }
  ```
>Note: Prior to KE version 4.5.17.0, the `total_size` value was the total number of tables the requesting user had permissions on in the project. Due to performance issues, in KE 4.5.17.0 and later, the `total_size` value is adjusted to the total number of all loaded tables in the project and no longer correlates to the actual table permissions the requesting user has in the project.
  
