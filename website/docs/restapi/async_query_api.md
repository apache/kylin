---
title: Async Query API
language: en
sidebar_label: Async Query API
pagination_label: Async Query API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - async query api
draft: false
last_update:
    date: 08/12/2022
---

> Reminders:
>
> 1. Please read [Access and Authentication REST API](authentication.md) and understand how authentication works.
> 2. On Curl command line, don't forget to quote the URL if it contains the special char `&`.
> 3. In this chapter， all get requests and delete by ID request are forward compatible, that is, if the project parameter is not transferred to the URL, the request can also be successful if the project parameter is transferred to the request body.
> 4. Due to the requirements of the parquet format, the naming of the column can not contain characters such as ",; {} () \\ n\\ t =", so you need to display the definition alias in the SQL query, and the alias does not contain these special characters.
> 5. The earlier versions of hive do not support reading the parquet date type field, so you can replace the original table data type with timestamp, or upgrade hive to the versions after 1.2 .



### Submit Async Query   {#Submit-Async-Query}

- `POST http://host:port/kylin/api/async_query`

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object
  - `sql` - `required` `string`, SQL statement
  - `separator` - `optional` `string`, separator of the exported result, which is  `,` by default. Other separators such as `#`, `$`, `@`, `|` are also supported.
  - `offset` - `optional` `int`, offset of query result
  - `limit` - `optional` `int `, limit on the quantity of query result
  - `project` - `required` `string`, project name
  - `format` - `optional` `string`，file format, the default value is "csv", other optional values are "json", "xlsx", "parquet"
     > Note: When the file format is "xlsx" or "json", specifying the separator separator is not supported. When the file format is "parquet", it is currently not supported to download the result file through the [Download Query Result](#Download-Query-Result) API, and can only be obtained directly from HDFS.
  - `encode` - `optional` `string`，file encoding, the default value is "utf-8", other optional values are "gbk"
  - `file_name` - `optional` `string`，file name, Chinese is not supported temporarily, the default value is "result"
  - `spark_queue` - `optional` `string`, the cluster queue specified, the default value is "default". It will take effect after enabling `kylin.query.unique-async-query-yarn-queue-enabled`

- Curl Request Example

  ```sh
  curl -X POST \
    'http://host:port/kylin/api/async_query' \
    -H 'Accept: application/vnd.apache.kylin-v4+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{ "sql":"select * from KYLIN_SALES limit 100", "project":"learn_kylin" }'
  ```


- Response Example

  ```json
  {
      "code": "000",
      "data": {
          "query_id": "eb3e837f-d826-4670-aac7-2b92fcd0c8fe",
          "status": "RUNNING",
          "info": "query still running"
      },
      "msg": ""
  }
  ```

- Response Information

  - `query_id` -  Query ID of the Async Query
  - `status` - Status, ie., "SUCCESSFUL", "FAILED", "RUNNING"
  - `info` - Detailed information about the status 



### Request Query Status   {#Request-Query-Status}

- `GET http://host:port/kylin/api/async_query/{query_id}/status?project=learn_kylin`

- URL Parameters

  - `query_id` - `required` `string`, Query ID of the Async Query
  - `project` `required` `string`,  project name
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`
  
- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/async_query/{query_id}/status?project=learn_kylin' \
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
          "query_id": "eb3e837f-d826-4670-aac7-2b92fcd0c8fe",
          "status": "SUCCESSFUL",
          "info": "await fetching results"
      },
      "msg": ""
  }
  ```

- Response Information
  - `query_id` - Query ID of the Async Query
  - `status` - Status, ie., "SUCCESSFUL" , "RUNNING", "FAILED" and "MISSING" 
  - `info` - Detailed information about the status



### Request Query Metadata Info {#Request-Query-Metadata-Info}

- `GET http://host:port/kylin/api/async_query/{query_id}/metadata?project=learn_kylin`

- URL Parameters
  - `query_id` - `required` `string`,  Query ID of the Async Query
  - `project` `required` `string`,  project name
  
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`
  
- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/async_query/{query_id}/metadata?project=learn_kylin' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```


- Response Example

  ```json
  {
      "code": "000",
      "data": [
          [
              "TRANS_ID",
              "PART_DT"
          ],
          [
              "BIGINT",
              "DATE"
          ]
      ],
      "msg": ""
  }
  ```

- Response Information

  - `data` - data includes two  list, the first list is the column name, and the second list is the corresponding data type of the column



### Request Query Result File Status {#Request-Query-Result-File-Status}

- `GET http://host:port/kylin/api/async_query/{query_id}/file_status?project=learn_kylin`


- URL Parameters
  - `query_id` - `required` `string`, Query ID of the Async Query
  - `project` `required` `string`,  project name

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`
  
- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/async_query/{query_id}/file_status?project=learn_kylin`' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```
  
- Response Example

  ```json
  {
      "code": "000",
      "data": 7611,
      "msg": ""
  }
  ```

- Response Information

  - `data` - total size of the result



### Download Query Result {#Download-Query-Result}

> Note: Please make sure the query status is "SUCCESSFUL" before calling this API.

- `GET http://host:port/kylin/api/async_query/{query_id}/result_download?include_header=true&project=learn_kylin`


- URL Parameters
  - `query_id` - `required` `string`,  Query ID of the Async Query
  - `include_header` - `optional` `boolean`, result include header, default is false
  - `project` `required` `string`,  project name
  
- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example


  ```sh
  curl -X GET \
    'http://host:port/kylin/api/async_query/{query_id}/result_download?include_header=true&project=learn_kylin' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -o result.csv
  ```

- Response Example
  - returns a document named `result.csv`.




### Request Query HDFS Path  {#Request-Query-HDFS-Path}

- `GET http://host:port/kylin/api/async_query/{query_id}/result_path?project=learn_kylin`


- URL Parameters
  - `query_id` - `required` `string`,  Query ID of the Async Query
  - `project` `required` `string`,  project name

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`
  
- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/async_query/{query_id}/result_path?project=learn_kylin' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```


- Response Example

  ```json
  {
      "code": "000",
      "data": "hdfs://host:8020/{kylin_working_dir}/{kylin_metadata_url}/{project}/async_query_result/{query_id}",
      "msg": ""
  }
  ```

- Response Information

  - `data` -  the HDFS Path in which stores the result file



### Delete All Query Result Files  {#Delete-All-Query-Result-Files}

- `DELETE http://host:port/kylin/api/async_query`


- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X DELETE \
    'http://host:port/kylin/api/async_query' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Example

  ```json
  {
      "code": "000",
      "data": true,
      "msg": ""
  }
  ```



### Delete old query result files based on time  {#Delete-old-query-result-files-based-on-time}

> Tip: This interface may delete queries that have not yet obtained results.

- `DELETE http://host:port/kylin/api/async_query?project={project}&older_than={time}`

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- URL Parameters

  - `project`-`required` `string`, the query result file of which project needs to be deleted
  - `Older_than`-`required` `string`, the earliest retention time, the asynchronous query result file whose last_modify is earlier than this time will be deleted, the time format is `yyyy-MM-dd HH:mm:ss`, no need to bring it quotation marks. Note: When using Curl request, it needs to perform url escaping by replacing spaces with `%20`.

- Curl request example

  ```sh
  curl -X DELETE \
    'http://host:port/kylin/api/async_query?project=learn_kylin&older_than=2021-04-26%2010:00:00' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```


- Response example

  ```json
  {
      "code": "000",
      "data": true,
      "msg": ""
  }
  ```

- Response information

  - `data`-Returns true if all old query result files are successfully deleted, otherwise false



### Delete query result files according to query_id  {#Delete-query-result-file-based-on-query_id}

- `DELETE http://host:port/kylin/api/async_query/{query_id}?project=learn_kylin`

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`
  
- URL Parameters
  - `query_id`-`required ` `string`, Query ID for asynchronous query
  - `project`-`required` `string`,  project name

- Curl request example

  ```sh
  curl -X DELETE \
    'http://host:port/kylin/api/async_query/{query_id}?project=learn_kylin' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response example

  ```json
  {
      "code": "000",
      "data": true,
      "msg": ""
  }
  ```

- Response information

  - `data`-Return true if the query result file corresponding to `query_id` is successfully deleted, otherwise false

### Known Limitations
- When the column name contains a comma `,`, if the separator `separator` is specified, the comma `,` in the column name of the header of the download result table will be replaced by the separator.
- When column names contain separator, the file format may be parsed incorrectly.
