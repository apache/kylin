---
layout: docs15
title:  Use RESTful API
categories: howto
permalink: /docs15/howto/howto_use_restapi.html
since: v0.7.1
---

This page lists all the RESTful APIs provided by Kylin; The base of the URL is `/kylin/api`, so don't forget to add it before a certain API's path. For example, to get all cube instances, send HTTP GET request to "/kylin/api/cubes".

* Query
   * [Authentication](#authentication)
   * [Query](#query)
   * [List queryable tables](#list-queryable-tables)
* CUBE
   * [List cubes](#list-cubes)
   * [Get cube](#get-cube)
   * [Get cube descriptor (dimension, measure info, etc)](#get-cube-descriptor)
   * [Get data model (fact and lookup table info)](#get-data-model)
   * [Build cube](#build-cube)
   * [Disable cube](#disable-cube)
   * [Purge cube](#purge-cube)
   * [Enable cube](#enable-cube)
* JOB
   * [Resume job](#resume-job)
   * [Discard job](#discard-job)
   * [Get job step output](#get-job-step-output)
* Metadata
   * [Get Hive Table](#get-hive-table)
   * [Get Hive Table (Extend Info)](#get-hive-table-extend-info)
   * [Get Hive Tables](#get-hive-tables)
   * [Load Hive Tables](#load-hive-tables)
* Cache
   * [Wipe cache](#wipe-cache)

## Authentication
`POST /user/authentication`

#### Request Header
Authorization data encoded by basic auth is needed in the header, such as:
Authorization:Basic {data}

#### Response Body
* userDetails - Defined authorities and status of current user.

#### Response Sample

```sh
{  
   "userDetails":{  
      "password":null,
      "username":"sample",
      "authorities":[  
         {  
            "authority":"ROLE_ANALYST"
         },
         {  
            "authority":"ROLE_MODELER"
         }
      ],
      "accountNonExpired":true,
      "accountNonLocked":true,
      "credentialsNonExpired":true,
      "enabled":true
   }
}
```

Example with `curl`: 

```
curl -c /path/to/cookiefile.txt -X POST -H "Authorization: Basic XXXXXXXXX" -H 'Content-Type: application/json' http://<host>:<port>/kylin/api/user/authentication
```

If login successfully, the JSESSIONID will be saved into the cookie file; In the subsequent http requests, attach the cookie, for example:

```
curl -b /path/to/cookiefile.txt -X PUT -H 'Content-Type: application/json' -d '{"startTime":'1423526400000', "endTime":'1423526400', "buildType":"BUILD"}' http://<host>:<port>/kylin/api/cubes/your_cube/rebuild
```

***

## Query
`POST /query`

#### Request Body
* sql - `required` `string` The text of sql statement.
* offset - `optional` `int` Query offset. If offset is set in sql, curIndex will be ignored.
* limit - `optional` `int` Query limit. If limit is set in sql, perPage will be ignored.
* acceptPartial - `optional` `bool` Whether accept a partial result or not, default be "false". Set to "false" for production use. 
* project - `optional` `string` Project to perform query. Default value is 'DEFAULT'.

#### Request Sample

```sh
{  
   "sql":"select * from TEST_KYLIN_FACT",
   "offset":0,
   "limit":50000,
   "acceptPartial":false,
   "project":"DEFAULT"
}
```

#### Response Body
* columnMetas - Column metadata information of result set.
* results - Data set of result.
* cube - Cube used for this query.
* affectedRowCount - Count of affected row by this sql statement.
* isException - Whether this response is an exception.
* ExceptionMessage - Message content of the exception.
* Duration - Time cost of this query
* Partial - Whether the response is a partial result or not. Decided by `acceptPartial` of request.

#### Response Sample

```sh
{  
   "columnMetas":[  
      {  
         "isNullable":1,
         "displaySize":0,
         "label":"CAL_DT",
         "name":"CAL_DT",
         "schemaName":null,
         "catelogName":null,
         "tableName":null,
         "precision":0,
         "scale":0,
         "columnType":91,
         "columnTypeName":"DATE",
         "readOnly":true,
         "writable":false,
         "caseSensitive":true,
         "searchable":false,
         "currency":false,
         "signed":true,
         "autoIncrement":false,
         "definitelyWritable":false
      },
      {  
         "isNullable":1,
         "displaySize":10,
         "label":"LEAF_CATEG_ID",
         "name":"LEAF_CATEG_ID",
         "schemaName":null,
         "catelogName":null,
         "tableName":null,
         "precision":10,
         "scale":0,
         "columnType":4,
         "columnTypeName":"INTEGER",
         "readOnly":true,
         "writable":false,
         "caseSensitive":true,
         "searchable":false,
         "currency":false,
         "signed":true,
         "autoIncrement":false,
         "definitelyWritable":false
      }
   ],
   "results":[  
      [  
         "2013-08-07",
         "32996",
         "15",
         "15",
         "Auction",
         "10000000",
         "49.048952730908745",
         "49.048952730908745",
         "49.048952730908745",
         "1"
      ],
      [  
         "2013-08-07",
         "43398",
         "0",
         "14",
         "ABIN",
         "10000633",
         "85.78317064220418",
         "85.78317064220418",
         "85.78317064220418",
         "1"
      ]
   ],
   "cube":"test_kylin_cube_with_slr_desc",
   "affectedRowCount":0,
   "isException":false,
   "exceptionMessage":null,
   "duration":3451,
   "partial":false
}
```

## List queryable tables
`GET /tables_and_columns`

#### Request Parameters
* project - `required` `string` The project to load tables

#### Response Sample
```sh
[  
   {  
      "columns":[  
         {  
            "table_NAME":"TEST_CAL_DT",
            "table_SCHEM":"EDW",
            "column_NAME":"CAL_DT",
            "data_TYPE":91,
            "nullable":1,
            "column_SIZE":-1,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":-1,
            "ordinal_POSITION":1,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"DATE"
         },
         {  
            "table_NAME":"TEST_CAL_DT",
            "table_SCHEM":"EDW",
            "column_NAME":"WEEK_BEG_DT",
            "data_TYPE":91,
            "nullable":1,
            "column_SIZE":-1,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":-1,
            "ordinal_POSITION":2,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"DATE"
         }
      ],
      "table_NAME":"TEST_CAL_DT",
      "table_SCHEM":"EDW",
      "ref_GENERATION":null,
      "self_REFERENCING_COL_NAME":null,
      "type_SCHEM":null,
      "table_TYPE":"TABLE",
      "table_CAT":"defaultCatalog",
      "remarks":null,
      "type_CAT":null,
      "type_NAME":null
   }
]
```

***

## List cubes
`GET /cubes`

#### Request Parameters
* offset - `required` `int` Offset used by pagination
* limit - `required` `int ` Cubes per page.
* cubeName - `optional` `string` Keyword for cube names. To find cubes whose name contains this keyword.
* projectName - `optional` `string` Project name.

#### Response Sample
```sh
[  
   {  
      "uuid":"1eaca32a-a33e-4b69-83dd-0bb8b1f8c53b",
      "last_modified":1407831634847,
      "name":"test_kylin_cube_with_slr_empty",
      "owner":null,
      "version":null,
      "descriptor":"test_kylin_cube_with_slr_desc",
      "cost":50,
      "status":"DISABLED",
      "segments":[  
      ],
      "create_time":null,
      "source_records_count":0,
      "source_records_size":0,
      "size_kb":0
   }
]
```

## Get cube
`GET /cubes/{cubeName}`

#### Path Variable
* cubeName - `required` `string` Cube name to find.

## Get cube descriptor
`GET /cube_desc/{cubeName}`
Get descriptor for specified cube instance.

#### Path Variable
* cubeName - `required` `string` Cube name.

#### Response Sample
```sh
[
    {
        "uuid": "a24ca905-1fc6-4f67-985c-38fa5aeafd92", 
        "name": "test_kylin_cube_with_slr_desc", 
        "description": null, 
        "dimensions": [
            {
                "id": 0, 
                "name": "CAL_DT", 
                "table": "EDW.TEST_CAL_DT", 
                "column": null, 
                "derived": [
                    "WEEK_BEG_DT"
                ], 
                "hierarchy": false
            }, 
            {
                "id": 1, 
                "name": "CATEGORY", 
                "table": "DEFAULT.TEST_CATEGORY_GROUPINGS", 
                "column": null, 
                "derived": [
                    "USER_DEFINED_FIELD1", 
                    "USER_DEFINED_FIELD3", 
                    "UPD_DATE", 
                    "UPD_USER"
                ], 
                "hierarchy": false
            }, 
            {
                "id": 2, 
                "name": "CATEGORY_HIERARCHY", 
                "table": "DEFAULT.TEST_CATEGORY_GROUPINGS", 
                "column": [
                    "META_CATEG_NAME", 
                    "CATEG_LVL2_NAME", 
                    "CATEG_LVL3_NAME"
                ], 
                "derived": null, 
                "hierarchy": true
            }, 
            {
                "id": 3, 
                "name": "LSTG_FORMAT_NAME", 
                "table": "DEFAULT.TEST_KYLIN_FACT", 
                "column": [
                    "LSTG_FORMAT_NAME"
                ], 
                "derived": null, 
                "hierarchy": false
            }, 
            {
                "id": 4, 
                "name": "SITE_ID", 
                "table": "EDW.TEST_SITES", 
                "column": null, 
                "derived": [
                    "SITE_NAME", 
                    "CRE_USER"
                ], 
                "hierarchy": false
            }, 
            {
                "id": 5, 
                "name": "SELLER_TYPE_CD", 
                "table": "EDW.TEST_SELLER_TYPE_DIM", 
                "column": null, 
                "derived": [
                    "SELLER_TYPE_DESC"
                ], 
                "hierarchy": false
            }, 
            {
                "id": 6, 
                "name": "SELLER_ID", 
                "table": "DEFAULT.TEST_KYLIN_FACT", 
                "column": [
                    "SELLER_ID"
                ], 
                "derived": null, 
                "hierarchy": false
            }
        ], 
        "measures": [
            {
                "id": 1, 
                "name": "GMV_SUM", 
                "function": {
                    "expression": "SUM", 
                    "parameter": {
                        "type": "column", 
                        "value": "PRICE", 
                        "next_parameter": null
                    }, 
                    "returntype": "decimal(19,4)"
                }, 
                "dependent_measure_ref": null
            }, 
            {
                "id": 2, 
                "name": "GMV_MIN", 
                "function": {
                    "expression": "MIN", 
                    "parameter": {
                        "type": "column", 
                        "value": "PRICE", 
                        "next_parameter": null
                    }, 
                    "returntype": "decimal(19,4)"
                }, 
                "dependent_measure_ref": null
            }, 
            {
                "id": 3, 
                "name": "GMV_MAX", 
                "function": {
                    "expression": "MAX", 
                    "parameter": {
                        "type": "column", 
                        "value": "PRICE", 
                        "next_parameter": null
                    }, 
                    "returntype": "decimal(19,4)"
                }, 
                "dependent_measure_ref": null
            }, 
            {
                "id": 4, 
                "name": "TRANS_CNT", 
                "function": {
                    "expression": "COUNT", 
                    "parameter": {
                        "type": "constant", 
                        "value": "1", 
                        "next_parameter": null
                    }, 
                    "returntype": "bigint"
                }, 
                "dependent_measure_ref": null
            }, 
            {
                "id": 5, 
                "name": "ITEM_COUNT_SUM", 
                "function": {
                    "expression": "SUM", 
                    "parameter": {
                        "type": "column", 
                        "value": "ITEM_COUNT", 
                        "next_parameter": null
                    }, 
                    "returntype": "bigint"
                }, 
                "dependent_measure_ref": null
            }
        ], 
        "rowkey": {
            "rowkey_columns": [
                {
                    "column": "SELLER_ID", 
                    "length": 18, 
                    "dictionary": null, 
                    "mandatory": true
                }, 
                {
                    "column": "CAL_DT", 
                    "length": 0, 
                    "dictionary": "true", 
                    "mandatory": false
                }, 
                {
                    "column": "LEAF_CATEG_ID", 
                    "length": 0, 
                    "dictionary": "true", 
                    "mandatory": false
                }, 
                {
                    "column": "META_CATEG_NAME", 
                    "length": 0, 
                    "dictionary": "true", 
                    "mandatory": false
                }, 
                {
                    "column": "CATEG_LVL2_NAME", 
                    "length": 0, 
                    "dictionary": "true", 
                    "mandatory": false
                }, 
                {
                    "column": "CATEG_LVL3_NAME", 
                    "length": 0, 
                    "dictionary": "true", 
                    "mandatory": false
                }, 
                {
                    "column": "LSTG_FORMAT_NAME", 
                    "length": 12, 
                    "dictionary": null, 
                    "mandatory": false
                }, 
                {
                    "column": "LSTG_SITE_ID", 
                    "length": 0, 
                    "dictionary": "true", 
                    "mandatory": false
                }, 
                {
                    "column": "SLR_SEGMENT_CD", 
                    "length": 0, 
                    "dictionary": "true", 
                    "mandatory": false
                }
            ], 
            "aggregation_groups": [
                [
                    "LEAF_CATEG_ID", 
                    "META_CATEG_NAME", 
                    "CATEG_LVL2_NAME", 
                    "CATEG_LVL3_NAME", 
                    "CAL_DT"
                ]
            ]
        }, 
        "signature": "lsLAl2jL62ZApmOLZqWU3g==", 
        "last_modified": 1445850327000, 
        "model_name": "test_kylin_with_slr_model_desc", 
        "null_string": null, 
        "hbase_mapping": {
            "column_family": [
                {
                    "name": "F1", 
                    "columns": [
                        {
                            "qualifier": "M", 
                            "measure_refs": [
                                "GMV_SUM", 
                                "GMV_MIN", 
                                "GMV_MAX", 
                                "TRANS_CNT", 
                                "ITEM_COUNT_SUM"
                            ]
                        }
                    ]
                }
            ]
        }, 
        "notify_list": null, 
        "auto_merge_time_ranges": null, 
        "retention_range": 0
    }
]
```

## Get data model
`GET /model/{modelName}`

#### Path Variable
* modelName - `required` `string` Data model name, by default it should be the same with cube name.

#### Response Sample
```sh
{
    "uuid": "ff527b94-f860-44c3-8452-93b17774c647", 
    "name": "test_kylin_with_slr_model_desc", 
    "lookups": [
        {
            "table": "EDW.TEST_CAL_DT", 
            "join": {
                "type": "inner", 
                "primary_key": [
                    "CAL_DT"
                ], 
                "foreign_key": [
                    "CAL_DT"
                ]
            }
        }, 
        {
            "table": "DEFAULT.TEST_CATEGORY_GROUPINGS", 
            "join": {
                "type": "inner", 
                "primary_key": [
                    "LEAF_CATEG_ID", 
                    "SITE_ID"
                ], 
                "foreign_key": [
                    "LEAF_CATEG_ID", 
                    "LSTG_SITE_ID"
                ]
            }
        }
    ], 
    "capacity": "MEDIUM", 
    "last_modified": 1442372116000, 
    "fact_table": "DEFAULT.TEST_KYLIN_FACT", 
    "filter_condition": null, 
    "partition_desc": {
        "partition_date_column": "DEFAULT.TEST_KYLIN_FACT.CAL_DT", 
        "partition_date_start": 0, 
        "partition_date_format": "yyyy-MM-dd", 
        "partition_type": "APPEND", 
        "partition_condition_builder": "org.apache.kylin.metadata.model.PartitionDesc$DefaultPartitionConditionBuilder"
    }
}
```

## Build cube
`PUT /cubes/{cubeName}/rebuild`

#### Path Variable
* cubeName - `required` `string` Cube name.

#### Request Body
* startTime - `required` `long` Start timestamp of data to build, e.g. 1388563200000 for 2014-1-1
* endTime - `required` `long` End timestamp of data to build
* buildType - `required` `string` Supported build type: 'BUILD', 'MERGE', 'REFRESH'

#### Response Sample
```
{  
   "uuid":"c143e0e4-ac5f-434d-acf3-46b0d15e3dc6",
   "last_modified":1407908916705,
   "name":"test_kylin_cube_with_slr_empty - 19700101000000_20140731160000 - BUILD - PDT 2014-08-12 22:48:36",
   "type":"BUILD",
   "duration":0,
   "related_cube":"test_kylin_cube_with_slr_empty",
   "related_segment":"19700101000000_20140731160000",
   "exec_start_time":0,
   "exec_end_time":0,
   "mr_waiting":0,
   "steps":[  
      {  
         "interruptCmd":null,
         "name":"Create Intermediate Flat Hive Table",
         "sequence_id":0,
         "exec_cmd":"hive -e \"DROP TABLE IF EXISTS kylin_intermediate_test_kylin_cube_with_slr_desc_19700101000000_20140731160000_c143e0e4_ac5f_434d_acf3_46b0d15e3dc6;\nCREATE EXTERNAL TABLE IF NOT EXISTS kylin_intermediate_test_kylin_cube_with_slr_desc_19700101000000_20140731160000_c143e0e4_ac5f_434d_acf3_46b0d15e3dc6\n(\nCAL_DT date\n,LEAF_CATEG_ID int\n,LSTG_SITE_ID int\n,META_CATEG_NAME string\n,CATEG_LVL2_NAME string\n,CATEG_LVL3_NAME string\n,LSTG_FORMAT_NAME string\n,SLR_SEGMENT_CD smallint\n,SELLER_ID bigint\n,PRICE decimal\n)\nROW FORMAT DELIMITED FIELDS TERMINATED BY '\\177'\nSTORED AS SEQUENCEFILE\nLOCATION '/tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/kylin_intermediate_test_kylin_cube_with_slr_desc_19700101000000_20140731160000_c143e0e4_ac5f_434d_acf3_46b0d15e3dc6';\nSET mapreduce.job.split.metainfo.maxsize=-1;\nSET mapred.compress.map.output=true;\nSET mapred.map.output.compression.codec=com.hadoop.compression.lzo.LzoCodec;\nSET mapred.output.compress=true;\nSET mapred.output.compression.codec=com.hadoop.compression.lzo.LzoCodec;\nSET mapred.output.compression.type=BLOCK;\nSET mapreduce.job.max.split.locations=2000;\nSET hive.exec.compress.output=true;\nSET hive.auto.convert.join.noconditionaltask = true;\nSET hive.auto.convert.join.noconditionaltask.size = 300000000;\nINSERT OVERWRITE TABLE kylin_intermediate_test_kylin_cube_with_slr_desc_19700101000000_20140731160000_c143e0e4_ac5f_434d_acf3_46b0d15e3dc6\nSELECT\nTEST_KYLIN_FACT.CAL_DT\n,TEST_KYLIN_FACT.LEAF_CATEG_ID\n,TEST_KYLIN_FACT.LSTG_SITE_ID\n,TEST_CATEGORY_GROUPINGS.META_CATEG_NAME\n,TEST_CATEGORY_GROUPINGS.CATEG_LVL2_NAME\n,TEST_CATEGORY_GROUPINGS.CATEG_LVL3_NAME\n,TEST_KYLIN_FACT.LSTG_FORMAT_NAME\n,TEST_KYLIN_FACT.SLR_SEGMENT_CD\n,TEST_KYLIN_FACT.SELLER_ID\n,TEST_KYLIN_FACT.PRICE\nFROM TEST_KYLIN_FACT\nINNER JOIN TEST_CAL_DT\nON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\nINNER JOIN TEST_CATEGORY_GROUPINGS\nON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\nINNER JOIN TEST_SITES\nON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\nINNER JOIN TEST_SELLER_TYPE_DIM\nON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD\nWHERE (test_kylin_fact.cal_dt < '2014-07-31 16:00:00')\n;\n\"",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"SHELL_CMD_HADOOP",
         "info":null,
         "run_async":false
      },
      {  
         "interruptCmd":null,
         "name":"Extract Fact Table Distinct Columns",
         "sequence_id":1,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/kylin_intermediate_test_kylin_cube_with_slr_desc_19700101000000_20140731160000_c143e0e4_ac5f_434d_acf3_46b0d15e3dc6 -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/fact_distinct_columns -jobname Kylin_Fact_Distinct_Columns_test_kylin_cube_with_slr_empty_Step_1",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_FACTDISTINCT",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Load HFile to HBase Table",
         "sequence_id":12,
         "exec_cmd":" -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/hfile/ -htablename KYLIN-CUBE-TEST_KYLIN_CUBE_WITH_SLR_EMPTY-19700101000000_20140731160000_11BB4326-5975-4358-804C-70D53642E03A -cubename test_kylin_cube_with_slr_empty",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NO_MR_BULKLOAD",
         "info":null,
         "run_async":false
      }
   ],
   "job_status":"PENDING",
   "progress":0.0
}
```

## Enable Cube
`PUT /cubes/{cubeName}/enable`

#### Path variable
* cubeName - `required` `string` Cube name.

#### Response Sample
```sh
{  
   "uuid":"1eaca32a-a33e-4b69-83dd-0bb8b1f8c53b",
   "last_modified":1407909046305,
   "name":"test_kylin_cube_with_slr_ready",
   "owner":null,
   "version":null,
   "descriptor":"test_kylin_cube_with_slr_desc",
   "cost":50,
   "status":"ACTIVE",
   "segments":[  
      {  
         "name":"19700101000000_20140531160000",
         "storage_location_identifier":"KYLIN-CUBE-TEST_KYLIN_CUBE_WITH_SLR_READY-19700101000000_20140531160000_BF043D2D-9A4A-45E9-AA59-5A17D3F34A50",
         "date_range_start":0,
         "date_range_end":1401552000000,
         "status":"READY",
         "size_kb":4758,
         "source_records":6000,
         "source_records_size":620356,
         "last_build_time":1407832663227,
         "last_build_job_id":"2c7a2b63-b052-4a51-8b09-0c24b5792cda",
         "binary_signature":null,
         "dictionaries":{  
            "TEST_CATEGORY_GROUPINGS/CATEG_LVL2_NAME":"/dict/TEST_CATEGORY_GROUPINGS/CATEG_LVL2_NAME/16d8185c-ee6b-4f8c-a919-756d9809f937.dict",
            "TEST_KYLIN_FACT/LSTG_SITE_ID":"/dict/TEST_SITES/SITE_ID/0bec6bb3-1b0d-469c-8289-b8c4ca5d5001.dict",
            "TEST_KYLIN_FACT/SLR_SEGMENT_CD":"/dict/TEST_SELLER_TYPE_DIM/SELLER_TYPE_CD/0c5d77ec-316b-47e0-ba9a-0616be890ad6.dict",
            "TEST_KYLIN_FACT/CAL_DT":"/dict/PREDEFINED/date(yyyy-mm-dd)/64ac4f82-f2af-476e-85b9-f0805001014e.dict",
            "TEST_CATEGORY_GROUPINGS/CATEG_LVL3_NAME":"/dict/TEST_CATEGORY_GROUPINGS/CATEG_LVL3_NAME/270fbfb0-281c-4602-8413-2970a7439c47.dict",
            "TEST_KYLIN_FACT/LEAF_CATEG_ID":"/dict/TEST_CATEGORY_GROUPINGS/LEAF_CATEG_ID/2602386c-debb-4968-8d2f-b52b8215e385.dict",
            "TEST_CATEGORY_GROUPINGS/META_CATEG_NAME":"/dict/TEST_CATEGORY_GROUPINGS/META_CATEG_NAME/0410d2c4-4686-40bc-ba14-170042a2de94.dict"
         },
         "snapshots":{  
            "TEST_CAL_DT":"/table_snapshot/TEST_CAL_DT.csv/8f7cfc8a-020d-4019-b419-3c6deb0ffaa0.snapshot",
            "TEST_SELLER_TYPE_DIM":"/table_snapshot/TEST_SELLER_TYPE_DIM.csv/c60fd05e-ac94-4016-9255-96521b273b81.snapshot",
            "TEST_CATEGORY_GROUPINGS":"/table_snapshot/TEST_CATEGORY_GROUPINGS.csv/363f4a59-b725-4459-826d-3188bde6a971.snapshot",
            "TEST_SITES":"/table_snapshot/TEST_SITES.csv/78e0aecc-3ec6-4406-b86e-bac4b10ea63b.snapshot"
         }
      }
   ],
   "create_time":null,
   "source_records_count":6000,
   "source_records_size":0,
   "size_kb":4758
}
```

## Disable Cube
`PUT /cubes/{cubeName}/disable`

#### Path variable
* cubeName - `required` `string` Cube name.

#### Response Sample
(Same as "Enable Cube")

## Purge Cube
`PUT /cubes/{cubeName}/purge`

#### Path variable
* cubeName - `required` `string` Cube name.

#### Response Sample
(Same as "Enable Cube")

***

## Resume Job
`PUT /jobs/{jobId}/resume`

#### Path variable
* jobId - `required` `string` Job id.

#### Response Sample
```
{  
   "uuid":"c143e0e4-ac5f-434d-acf3-46b0d15e3dc6",
   "last_modified":1407908916705,
   "name":"test_kylin_cube_with_slr_empty - 19700101000000_20140731160000 - BUILD - PDT 2014-08-12 22:48:36",
   "type":"BUILD",
   "duration":0,
   "related_cube":"test_kylin_cube_with_slr_empty",
   "related_segment":"19700101000000_20140731160000",
   "exec_start_time":0,
   "exec_end_time":0,
   "mr_waiting":0,
   "steps":[  
      {  
         "interruptCmd":null,
         "name":"Create Intermediate Flat Hive Table",
         "sequence_id":0,
         "exec_cmd":"hive -e \"DROP TABLE IF EXISTS kylin_intermediate_test_kylin_cube_with_slr_desc_19700101000000_20140731160000_c143e0e4_ac5f_434d_acf3_46b0d15e3dc6;\nCREATE EXTERNAL TABLE IF NOT EXISTS kylin_intermediate_test_kylin_cube_with_slr_desc_19700101000000_20140731160000_c143e0e4_ac5f_434d_acf3_46b0d15e3dc6\n(\nCAL_DT date\n,LEAF_CATEG_ID int\n,LSTG_SITE_ID int\n,META_CATEG_NAME string\n,CATEG_LVL2_NAME string\n,CATEG_LVL3_NAME string\n,LSTG_FORMAT_NAME string\n,SLR_SEGMENT_CD smallint\n,SELLER_ID bigint\n,PRICE decimal\n)\nROW FORMAT DELIMITED FIELDS TERMINATED BY '\\177'\nSTORED AS SEQUENCEFILE\nLOCATION '/tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/kylin_intermediate_test_kylin_cube_with_slr_desc_19700101000000_20140731160000_c143e0e4_ac5f_434d_acf3_46b0d15e3dc6';\nSET mapreduce.job.split.metainfo.maxsize=-1;\nSET mapred.compress.map.output=true;\nSET mapred.map.output.compression.codec=com.hadoop.compression.lzo.LzoCodec;\nSET mapred.output.compress=true;\nSET mapred.output.compression.codec=com.hadoop.compression.lzo.LzoCodec;\nSET mapred.output.compression.type=BLOCK;\nSET mapreduce.job.max.split.locations=2000;\nSET hive.exec.compress.output=true;\nSET hive.auto.convert.join.noconditionaltask = true;\nSET hive.auto.convert.join.noconditionaltask.size = 300000000;\nINSERT OVERWRITE TABLE kylin_intermediate_test_kylin_cube_with_slr_desc_19700101000000_20140731160000_c143e0e4_ac5f_434d_acf3_46b0d15e3dc6\nSELECT\nTEST_KYLIN_FACT.CAL_DT\n,TEST_KYLIN_FACT.LEAF_CATEG_ID\n,TEST_KYLIN_FACT.LSTG_SITE_ID\n,TEST_CATEGORY_GROUPINGS.META_CATEG_NAME\n,TEST_CATEGORY_GROUPINGS.CATEG_LVL2_NAME\n,TEST_CATEGORY_GROUPINGS.CATEG_LVL3_NAME\n,TEST_KYLIN_FACT.LSTG_FORMAT_NAME\n,TEST_KYLIN_FACT.SLR_SEGMENT_CD\n,TEST_KYLIN_FACT.SELLER_ID\n,TEST_KYLIN_FACT.PRICE\nFROM TEST_KYLIN_FACT\nINNER JOIN TEST_CAL_DT\nON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\nINNER JOIN TEST_CATEGORY_GROUPINGS\nON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\nINNER JOIN TEST_SITES\nON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\nINNER JOIN TEST_SELLER_TYPE_DIM\nON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD\nWHERE (test_kylin_fact.cal_dt < '2014-07-31 16:00:00')\n;\n\"",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"SHELL_CMD_HADOOP",
         "info":null,
         "run_async":false
      },
      {  
         "interruptCmd":null,
         "name":"Extract Fact Table Distinct Columns",
         "sequence_id":1,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/kylin_intermediate_test_kylin_cube_with_slr_desc_19700101000000_20140731160000_c143e0e4_ac5f_434d_acf3_46b0d15e3dc6 -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/fact_distinct_columns -jobname Kylin_Fact_Distinct_Columns_test_kylin_cube_with_slr_empty_Step_1",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_FACTDISTINCT",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Load HFile to HBase Table",
         "sequence_id":12,
         "exec_cmd":" -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/hfile/ -htablename KYLIN-CUBE-TEST_KYLIN_CUBE_WITH_SLR_EMPTY-19700101000000_20140731160000_11BB4326-5975-4358-804C-70D53642E03A -cubename test_kylin_cube_with_slr_empty",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NO_MR_BULKLOAD",
         "info":null,
         "run_async":false
      }
   ],
   "job_status":"PENDING",
   "progress":0.0
}
```

## Discard Job
`PUT /jobs/{jobId}/cancel`

#### Path variable
* jobId - `required` `string` Job id.

#### Response Sample
(Same as "Resume job")

## Get job step output
`GET /{jobId}/steps/{stepId}/output`

#### Path Variable
* jobId - `required` `string` Job id.
* stepId - `required` `string` Step id; the step id is composed by jobId with step sequence id; for example, the jobId is "fb479e54-837f-49a2-b457-651fc50be110", its 3rd step id is "fb479e54-837f-49a2-b457-651fc50be110-3", 

#### Response Sample
```
{  
   "cmd_output":"log string"
}
```

***

## Get Hive Table
`GET /tables/{tableName}`

#### Request Parameters
* tableName - `required` `string` table name to find.

#### Response Sample
```sh
{
    uuid: "69cc92c0-fc42-4bb9-893f-bd1141c91dbe",
    name: "SAMPLE_07",
    columns: [{
        id: "1",
        name: "CODE",
        datatype: "string"
    }, {
        id: "2",
        name: "DESCRIPTION",
        datatype: "string"
    }, {
        id: "3",
        name: "TOTAL_EMP",
        datatype: "int"
    }, {
        id: "4",
        name: "SALARY",
        datatype: "int"
    }],
    database: "DEFAULT",
    last_modified: 1419330476755
}
```

## Get Hive Table (Extend Info)
`GET /tables/{tableName}/exd-map`

#### Request Parameters
* tableName - `optional` `string` table name to find.

#### Response Sample
```
{
    "minFileSize": "46055",
    "totalNumberFiles": "1",
    "location": "hdfs://sandbox.hortonworks.com:8020/apps/hive/warehouse/sample_07",
    "lastAccessTime": "1418374103365",
    "lastUpdateTime": "1398176493340",
    "columns": "struct columns { string code, string description, i32 total_emp, i32 salary}",
    "partitionColumns": "",
    "EXD_STATUS": "true",
    "maxFileSize": "46055",
    "inputformat": "org.apache.hadoop.mapred.TextInputFormat",
    "partitioned": "false",
    "tableName": "sample_07",
    "owner": "hue",
    "totalFileSize": "46055",
    "outputformat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
}
```

## Get Hive Tables
`GET /tables`

#### Request Parameters
* project- `required` `string` will list all tables in the project.
* ext- `optional` `boolean`  set true to get extend info of table.

#### Response Sample
```sh
[
 {
    uuid: "53856c96-fe4d-459e-a9dc-c339b1bc3310",
    name: "SAMPLE_08",
    columns: [{
        id: "1",
        name: "CODE",
        datatype: "string"
    }, {
        id: "2",
        name: "DESCRIPTION",
        datatype: "string"
    }, {
        id: "3",
        name: "TOTAL_EMP",
        datatype: "int"
    }, {
        id: "4",
        name: "SALARY",
        datatype: "int"
    }],
    database: "DEFAULT",
    cardinality: {},
    last_modified: 0,
    exd: {
        minFileSize: "46069",
        totalNumberFiles: "1",
        location: "hdfs://sandbox.hortonworks.com:8020/apps/hive/warehouse/sample_08",
        lastAccessTime: "1398176495945",
        lastUpdateTime: "1398176495981",
        columns: "struct columns { string code, string description, i32 total_emp, i32 salary}",
        partitionColumns: "",
        EXD_STATUS: "true",
        maxFileSize: "46069",
        inputformat: "org.apache.hadoop.mapred.TextInputFormat",
        partitioned: "false",
        tableName: "sample_08",
        owner: "hue",
        totalFileSize: "46069",
        outputformat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    }
  }
]
```

## Load Hive Tables
`POST /tables/{tables}/{project}`

#### Request Parameters
* tables - `required` `string` table names you want to load from hive, separated with comma.
* project - `required` `String`  the project which the tables will be loaded into.

#### Response Sample
```
{
    "result.loaded": ["DEFAULT.SAMPLE_07"],
    "result.unloaded": ["sapmle_08"]
}
```

***

## Wipe cache
`PUT /cache/{type}/{name}/{action}`

#### Path variable
* type - `required` `string` 'METADATA' or 'CUBE'
* name - `required` `string` Cache key, e.g the cube name.
* action - `required` `string` 'create', 'update' or 'drop'

