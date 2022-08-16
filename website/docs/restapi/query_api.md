---
title: Query API
language: en
sidebar_label: Query API
pagination_label: Query API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - query api
draft: false
last_update:
    date: 08/12/2022
---

> Reminders:
>
> 1. Please read [Access and Authentication REST API](authentication.md) and understand how authentication works.
> 2. On Curl command line, don't forget to quote the URL if it contains the special char `&`.


### Query API {#Query-api}

- `POST http://host:port/kylin/api/query`

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object
  - `sql` - `required` `string`, SQL statement
  - `project` - `required` `string`, project name
  - `offset` - `optional` `int`, offset of query result. Must be used in conjunction with `limit`.
  - `limit` - `optional` `int`, limit on the quantity of returned query result
  - `forcedToPushDown` - `optional` `boolean`, whether to force queries to pushdown engine, `false` by default. You are not able to force queries to pushdown when the pushdown setting is turned off.
  - `partialMatchIndex` - `optional` `boolean`, `false` by default, whether to force queries the segments with proper index, when not all segment can answer the query
  - `forced_to_index` - `optional` `boolean`, whether to force the query to use index, default to `false`. When set to `true`, the query will return the result as normal if a matching index is found, otherwise, an error will be thrown and the query will not be pushded down.
  - `forcedToTieredStorage` - `optional` `int`, whether the query is forced to use tiered storage, the default value is `0`, which means that when the tiered storage cannot answer, it will be answered by the base table index on HDFS, configured as `1` indicates that when the tiered storage cannot answer the query, the query will be pushdown, configured as `2`, indicates that the query fails when the tiered storage cannot answer the query. When `forcedToPushDown` is `true`, this parameter doesn't take effect. When `forced_to_index` is `true`, this value  `1` doesn't take effect.
  - Parameters `forcedToPushDown` and `forced_to_index` cannot be `true` at the same time, an error will be reported.
  
- Curl Request Example

  ```sh
  curl -X POST \
    'http://host:port/kylin/api/query' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{ "sql":"select count(*) from SSB.P_LINEORDER", "project":"ssb", "partialMatchIndex":"true"}'
  ```

- Response Example

  ```json
  {
      "code":"000",
      "data":{
          "columnMetas":[
              {
                  "isNullable":0,
                  "displaySize":19,
                  "label":"EXPR$0",
                  "name":"EXPR$0",
                  "schemaName":null,
                  "catelogName":null,
                  "tableName":null,
                  "precision":19,
                  "scale":0,
                  "columnType":-5,
                  "columnTypeName":"BIGINT NOT NULL",
                  "caseSensitive":false,
                  "autoIncrement":false,
                  "currency":false,
                  "definitelyWritable":false,
                  "signed":true,
                  "writable":false,
                  "searchable":false,
                  "readOnly":false
              }
          ],
          "results":[
              [
                  "60175"
              ]
          ],
          "affectedRowCount":0,
          "exceptionMessage":null,
          "duration":129,
          "scanRows":[
              2519
          ],
          "totalScanRows":2519,
          "scanBytes":[
              6118
          ],
          "totalScanBytes":6118,
          "resultRowCount":1,
          "shufflePartitions":1,
          "hitExceptionCache":false,
          "storageCacheUsed":false,
          "queryStatistics":null,
          "traceUrl":null,
          "queryId":"738fad53-57a9-43fc-a186-b25d0993dfcb",
          "server":"client134.kcluster:7470",
          "suite":null,
          "signature":"1593670148521;1592230756490_1595228217256_1595228217256_1595228217256_1595228217257",
          "engineType":"NATIVE",
          "exception":false,
          "prepare":false,
          "timeout":false,
          "partial":false,
          "isException":false,
          "appMasterURL":"/kylin/sparder/SQL/execution/?id=27391",
          "pushDown":false,
          "is_prepare":false,
          "is_timeout":false,
          "realizations":[
              {
                  "modelId":"c7a7a1d0-71c6-4d42-bbc3-76167e5d2d10",
                  "modelAlias":"ssb_cube",
                  "layoutId":80001,
                  "indexType":"Agg Index",
                  "valid":true,
                  "partialMatchModel":false
              }
          ],
          "traces" : [
          {
             "name": "GET_ACL_INFO",
             "group": "PREPARATION",
             "duration": 10
          },
          {
             "name" : "SQL_TRANSFORMATION",
             "group" : "PREPARATION",
             "duration" : 3
          },
          {
             "name" : "SQL_PARSE_AND_OPTIMIZE",
             "group" : "PREPARATION",
             "duration" : 9
          },
          {
             "name" : "MODEL_MATCHING",
             "group" : "PREPARATION",
             "duration" : 2
          },
          {
             "name" : "PREPARE_AND_SUBMIT_JOB",
             "group" : null,
             "duration" : 64
          },
          {
             "name" : "WAIT_FOR_EXECUTION",
             "group" : null,
             "duration" : 2
          },
          {
             "name" : "EXECUTION",
             "group" : null,
             "duration" : 26
          },
          {
             "name" : "FETCH_RESULT",
             "group" : null,
             "duration" : 7
          }]
      },
      "msg":""
  }
  ```

- Response Information
  - `columnMetas` - metadata information of the columns
  - `results` - query results
  - `resultRowCount` - row count of query results
  - `isException` - whether the query returns exception
  - `exceptionMessage` - exception message
  - `queryId` - query ID
  - `duration` - query duration
  - `totalScanRows` - total scan count
  - `totalScanBytes` - total scan bytes
  - `hitExceptionCache` - whether hit the result cache of an exception query
  - `storageCacheUsed` - whether hit the result cache of a success query
  - `server` - which server executed this query
  - `timeout` - whether query is timeout
  - `pushDown` - whether query push down to other engine
  - `traces` - the trace information for each query execution stage
    - `name` - the stage name
    - `duration` - duration in milliseconds
    - `group` - the stage group

### Refresh cached data   {#Refresh-cached-data}

Calling this API will refresh the table cache of the Spark SQL Context for all query nodes. Application scenario: Query pushdown fails after the update of data source table.

- `PUT http://host:port/kylin/api/tables/catalog_cache`

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object

  - `tables` - `required` `string`,specify the table you want to load, in the format: DB.TABLE, separate multiple tables with commas

- Curl Request Example

  ```sh
  curl -X PUT \
    'http://host:port/kylin/api/tables/catalog_cache' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{"tables":["SSB.LINEORDER","SSB.SUPPLIER"]}'
  ```

- Response Example

  ```json
  {
      "code": "000",
      "data": {
          "nodes": [
              {
                  "server": "slave104.tnt:18001",
                  "refreshed": [
                      "SSB.LINEORDER",
                      "SSB.SUPPLIER"
                  ],
                  "failed": []
              },
              {
                  "server": "slave104.tnt:18003",
                  "refreshed": [
                      "SSB.LINEORDER",
                      "SSB.SUPPLIER"
                  ],
                  "failed": []
              }
          ]
      },
      "msg": ""
  }
  ```

- Response Information

  - `nodes` - Refresh results of different nodes
  - `server`  - Node information
  - `refreshed` - Table that refreshed successfully
  - `failed`  - Table that failed to refresh
  - `msg` - Reasons for refresh failure


### Get query histories   {#Get-query-histories}
- `GET http://host:port/kylin/api/query/query_histories`

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- URL Parameters
  - `project` - `required` `string`，project name
  - `page_offset` - `optional` `int`, offset of returned result, 0 by default
  - `page_size` - `optional` `int`, quantity of returned result per page, 10 by default
  - `start_time_from` - `optional` `string`，timestamp of query history start, cannot be used alone with start_time_to
  - `start_time_to` - `optional` `string`，timestamp of query history end, cannot be used alone with start_time_from

- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/query/query_histories?project=kylin_demo&page_offset=5&page_size=1&start_time_from=1656864000000&start_time_to=1656950400000' \
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
        "size": 79,
        "query_histories": [
            {
                "queryRealizations": null,
                "query_id": "4bfbc8f1-ffcb-ae86-c5f1-2b78c73cb802",
                "query_history_info": {
                    "exactly_match": true,
                    "scan_segment_num": 1,
                    "state": "FAILED",
                    "execution_error": false,
                    "error_msg": null,
                    "query_snapshots": [],
                    "realization_metrics": [
                        {
                            "queryId": "4bfbc8f1-ffcb-ae86-c5f1-2b78c73cb802",
                            "duration": 568,
                            "layoutId": "1",
                            "indexType": "Agg Index",
                            "modelId": "118ae12b-5198-ade2-ecd9-b7e5f64318a2",
                            "queryTime": 1656914360898,
                            "projectName": "kylin_demo",
                            "snapshots": [],
                            "secondStorage": false,
                            "streamingLayout": false
                        },
                        {
                            "queryId": "4bfbc8f1-ffcb-ae86-c5f1-2b78c73cb802",
                            "duration": 568,
                            "layoutId": "1",
                            "indexType": "Agg Index",
                            "modelId": "6cf4a660-e217-add5-87f4-04493c8df21e",
                            "queryTime": 1656914360898,
                            "projectName": "kylin_demo",
                            "snapshots": [],
                            "secondStorage": false,
                            "streamingLayout": false
                        }
                    ],
                    "traces": [
                        {
                            "name": "HTTP_RECEPTION",
                            "group": null,
                            "duration": 29
                        },
                        {
                            "name": "GET_ACL_INFO",
                            "group": "PREPARATION",
                            "duration": 1
                        },
                        {
                            "name": "SQL_TRANSFORMATION",
                            "group": "PREPARATION",
                            "duration": 12
                        },
                        {
                            "name": "SQL_PARSE_AND_OPTIMIZE",
                            "group": "PREPARATION",
                            "duration": 125
                        },
                        {
                            "name": "MODEL_MATCHING",
                            "group": "PREPARATION",
                            "duration": 10
                        },
                        {
                            "name": "PREPARE_AND_SUBMIT_JOB",
                            "group": "JOB_EXECUTION",
                            "duration": 277
                        },
                        {
                            "name": "WAIT_FOR_EXECUTION",
                            "group": "JOB_EXECUTION",
                            "duration": 13
                        },
                        {
                            "name": "EXECUTION",
                            "group": "JOB_EXECUTION",
                            "duration": 90
                        },
                        {
                            "name": "FETCH_RESULT",
                            "group": "JOB_EXECUTION",
                            "duration": 11
                        }
                    ],
                    "cache_type": null,
                    "query_msg": null
                },
                "sql_text": "SELECT *\nFROM\n  (SELECT PICKUP_DATE,\n          TAXI_ORDER_NUMBER,\n          PEOPLE_POSITIVE_NEW_CASES_COUNT,\n          round(TAXI_ORDER_NUMBER / PEOPLE_POSITIVE_NEW_CASES_COUNT, 2) AS COVID_IMPACT_INDEX\n   FROM\n     (SELECT PICKUP_DATE,\n             MONTH_START,\n             ZONE,\n             TAXI_ORDER_NUMBER\n      FROM\n        (SELECT PICKUP_DATE,\n                MONTH_START,\n                ZONE,\n                sum(TOTAL_AMOUNT) AS TAXI_PRICE_AMOUNT ,\n                count(TOTAL_AMOUNT) AS TAXI_ORDER_NUMBER\n         FROM\n           (SELECT PICKUP_DATE,\n                   MONTH_START,\n                   ZONE,\n                   TOTAL_AMOUNT,\n                   trip_distance\n            FROM KYLIN_DEMO.TAXI_TRIP_RECORDS_VIEW t_f\n            LEFT JOIN KYLIN_DEMO.NEWYORK_ZONE t_z ON t_f.PULOCATIONID = t_z.LOCATIONID\n            LEFT JOIN KYLIN_DEMO.LOOKUP_CALENDAR t_c ON t_f.PICKUP_DATE = t_c.DAY_START) t_merge\n         GROUP BY PICKUP_DATE,\n                  MONTH_START,\n                  ZONE)\n      WHERE ZONE = 'East New York') t_l\n   LEFT JOIN\n     (SELECT REPORT_DATE,\n             MONTH_START,\n             PROVINCE_STATE_NAME,\n             PEOPLE_POSITIVE_NEW_CASES_COUNT\n      FROM\n        (SELECT REPORT_DATE,\n                MONTH_START,\n                PROVINCE_STATE_NAME,\n                sum(PEOPLE_POSITIVE_NEW_CASES_COUNT) AS PEOPLE_POSITIVE_NEW_CASES_COUNT\n         FROM KYLIN_DEMO.COVID_19_ACTIVITY t_f\n         LEFT JOIN KYLIN_DEMO.LOOKUP_CALENDAR t_c ON t_f.REPORT_DATE = t_c.DAY_START\n         GROUP BY REPORT_DATE,\n                  MONTH_START,\n                  PROVINCE_STATE_NAME)\n      WHERE PROVINCE_STATE_NAME = 'New York') t_r ON t_l.PICKUP_DATE=t_r.REPORT_DATE) tt\nWHERE PICKUP_DATE >= '2020-01-01'\n  AND PICKUP_DATE <= '2020-12-31'\nLIMIT 50000",
                "query_time": 1656914360898,
                "duration": 568,
                "server": "snoopy-gw07.kylin.com:7095",
                "submitter": "ADMIN",
                "index_hit": true,
                "query_status": "SUCCEEDED",
                "result_row_count": 366,
                "id": 296,
                "engine_type": "NATIVE",
                "total_scan_count": 122839,
                "project_name": "kylin_demo",
                "realizations": [
                    {
                        "modelId": "118ae12b-5198-ade2-ecd9-b7e5f64318a2",
                        "modelAlias": "AUTO_MODEL_TAXI_TRIP_RECORDS_VIEW_1",
                        "layoutId": 1,
                        "indexType": "Agg Index",
                        "snapshots": [],
                        "valid": true,
                        "partialMatchModel": false
                    },
                    {
                        "modelId": "6cf4a660-e217-add5-87f4-04493c8df21e",
                        "modelAlias": "AUTO_MODEL_COVID_19_ACTIVITY_1",
                        "layoutId": 1,
                        "indexType": "Agg Index",
                        "snapshots": [],
                        "valid": true,
                        "partialMatchModel": false
                    }
                ],
                "total_scan_bytes": 849962,
                "error_type": null,
                "cache_hit": false
            }
        ]
    },
    "msg": ""
}
```

- Response Information
  - `query_id` - query ID
  - `query_history_info` - Query history infomation
    - `exactly_match` - whether the query returns exception
    - `scan_segment_num` - scan segments
    - `state` - the flag whether the query satisfied query recommendation
    - `execution_error` - whether the query failed
    - `error_msg` - error message
    - `query_snapshots` - query snapshots
    - `realization_metrics`
      - `queryId` - query ID
      - `duration` - query duration(ms)
      - `layoutId` - index ID
      - `indexType` - index type
      - `modelId` - model ID
      - `queryTime` - timestamp of when query executed
      - `projectName` - project name
      - `snapshots` - snapshots
      - `secondStorage` - whether Tied Storage was used
      - `streamingLayout` - whether hit streaming index
    - `traces`
      - `name` - the stage name
      - `duration` - duration in milliseconds
      - `group` - the stage group
    - `cache_type` - cache type
    - `query_msg` - exception message
    - `sql_text` - SQL
    - `query_time` - timestamp of when query executed
    - `duration` - duration in milliseconds
    - `server` - which server executed this query
    - `submitter` - username
    - `index_hit` - whether the query hit index
    - `query_status` - query status
    - `result_row_count` - row count of query results
    - `engine_type` - engine type
    - `total_scan_count` - total scan counts
    - `project_name` - project name
    - `realizations`
      - `modelId` - model ID
      - `modelAlias` - model name
      - `layoutId` - index ID
      - `indexType` - index type
      - `snapshots` - snapshots
      - `valid` - whether the model is valid
      - `partialMatchModel` - whether using partial match
      - `total_scan_bytes` - total scan bytes
      - `error_type` - error type
      - `cache_hit` - whether hit cache



### Download query history SQL  {#Download-query-history-SQL}

- `GET  http://host:port/kylin/api/query/download_query_histories`

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`
  
- URL Parameters

    - `project` - `required` `string`, project name.
    - `timezone_offset_hour` - `required` `int`, query the time zone offset of history, the number of hours different from GMT, for example, East 8 is passed in 8, note that only the integer between [-18,18] can be taken.
    - `start_time_from` - `optional` `string`, query history start time timestamp, units ms, it work only when used together with start_time_to. For example, 1617206400000, if you enter a number other than a number, it will return empty.
    - `start_time_to` - `optional` `string`, query the end of history timestamp, units ms, it work only when used together with start_time_from. For example, 1620662400000, if you enter a number other than a number, it will return to null.
    - `latency_from` - `optional` `string`, query delay is greater than latency_from, units s, it work only when used together with latency_to. For example, 10, if you enter a number other than a number, it will return to null.
    - `latency_to` - `optional` `string`, query delay is less than latency_to, units s, it work only when used together with latency_from. For example, 20, if you enter a number other than a number, it will return to null.
    - `query_status` - `optional` `List<String>` query status, such as SUCCEEDED、FAILED. If you enter values other than these, it will return empty.
    - `sql` - `optional` `string`, used to fuzzy match user SQL or query ID.
    - `realization` - `optional` `List<string>` to query object.
    - `server` - `optional` `string`, the hostname and port of query node, e.g. myhost:7072 .
    - `submitter` - `optional` `List<string>` to query user.

- Example of curl request

    ```
    curl -X GET \
    ' http://host:port/kylin/api/query/download_ query_ histories?timezone_ offset_ hour=8&amp;realization=&amp;query_ status=&amp;submitter=&amp;project=default&amp;start_ time_ from=&amp;start_ time_ to=&amp;latency_ from=&amp;latency_ to=&amp;sql=' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -o query_history.csv
    ```


### Download query history SQL  {#Download-query-history-SQL}

- `GET  http://host:port/kylin/api/query/download_query_histories_sql`

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- URL Parameters

    - `project` - `required` `string`, project name
    - `start_time_from` - `optional` `string`, query history start time timestamp, units ms, it work only when used together with start_time_to. For example, 1617206400000, if you enter a number other than a number, it will return empty.
    - `start_time_to` - `optional` `string`, query the end of history timestamp, units ms, it work only when used together with start_time_from. For example, 1620662400000, if you enter a number other than a number, it will return to null.
    - `latency_from` - `optional` `string`, query delay is greater than latency_from, units s, it work only when used together with latency_to. For example, 10, if you enter a number other than a number, it will return to null.
    - `latency_to` - `optional` `string`, query delay is less than latency_to, units s, it work only when used together with latency_from. For example, 20, if you enter a number other than a number, it will return to null.
    - `query_status` - `optional` `List<string>` query status, such as succeeded and failed. If you enter values other than these, it will return null.
    - `sql` - `optional` `string`, used to fuzzy match user SQL or query ID.
    - `realization` - `optional` `List<string>` to query the object.
    - `server` - `optional` `string`, query node.
    - `submitter` - `optional` `List<string>` to query the user.

- Example of curl request

    ```
    curl -X GET \
    ' http://host:port/kylin/api/query/download_ query_ histories_ sql?timezone_ offset_ hour=8&amp;realization=&amp;query_ status=&amp;submitter=&amp;project=default&amp;start_ time_ from=&amp;start_ time_ to=&amp;latency_ from=&amp;latency_ to=&amp;sql=' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -o query_history.sql
    ```
