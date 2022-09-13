---
title: Model Management API
language: en
sidebar_label: Model Management API
pagination_label: Model Management API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - model management api
draft: false
last_update:
    date: 08/12/2022
---


> * On how authentication works, see [Access and Authentication REST API](../authentication.md).
> * On Curl command line, don't forget to quote the URL if it contains the special char `&`.
> * On Curl command line, escape `%25` if it contains the special char `%`.




### Create Model {#Create_Model}

- `POST http://host:port/kylin/api/models`

- Request Permission: MANAGEMENT permission and above

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object

  - `project` - `required` `string`, project name
  - `fact_table` - `required` `string`, fact table name
  - `uuid` -  `optional` `string`, model ID, default auto generated
  - `alias` -  `required` `string`, model alias name
  - `management_type` - `required` `string`, for model creation please use: MODEL_BASED
  - `simplified_measures` - `required` `JSON Object[]`, measures
    - `name` - `required` `string`, measure name
    - `expression` - `required` `string`, function, including: SUM,MIN,MAX,TOP_N,COUNT,COUNT_DISTINCT,CORR,PERCENTILE_APPROX,COLLECT_SET
    - `parameter_value` - `required` `JSON Object[]`, measure parameters
      - `type` - `required` `string`, parameter type: constant,column
      - `value` - `required` `int|string`, parameter value (Set value 1 when 'expression' is 'COUNT' and 'type' is 'constant', while others should be: TABLE.COLUMN)
    - `return_type` - `required` `string`, function return type with arguments including: topn(10)、topn(100)、percentile(100) etc., using empty string("") when no return type needed

      ```
      Supported return_type values:

      TOP_N:
        * Top 10: topn(10)
        * Top 100: topn(100)
        * Top 1000: topn(1000)

      COUNT_DISTINCT:
        * Error Rate < 9.75%: hllc(10)
        * Error Rate < 4.88%: hllc(12)
        * Error Rate < 2.44%: hllc(14)
        * Error Rate < 1.72%: hllc(15)
        * Error Rate < 1.22%: hllc(16)
        * Precisely: bitmap

      PERCENTILE_APPROX:
        * percentile(100)
        * percentile(1000)
        * percentile(10000)
      ```

    - `comment` -  `optional` `string`, comments
  - `simplified_dimensions` - `required` `JSON Object[]`, dimensions info
    - `column` - `required` `string`, format: TABLE.COLUMN
    - `name` - `required` `string`, column alias, may set the same as column name
    - `status` - `required` `string`, fixed value: DIMENSION
  - `computed_columns` -  `optional` `JSON Object[]`, computed columns info
    - `columnName` - `required` `string`, the new column name
    - `expression` - `required` `string`, expression
    - `datatype` - `required` `string`, data type: VARCHAR,INT,BIGINT ...standard sql types
    - `tableIdentity` - `required` `string`, format: SCHEMA.TABLE
    - `tableAlias` -  `required` `string`, table alias
  - `join_tables` -  `required` `JSON Object[]`, join info
    - `table` - `required` `string`, table name
    - `join` - `required` `JSON Object`, join info
      - `type` - `required` `string`, join type: INNER,LEFT
      - `foreign_key` - `required` `string[]`, foreign key
      - `primary_key` - `required` `string[]`, primary key
      - `simplified_non_equi_join_conditions` -  `optional` `JSON Object`, non-equivalent join conditions

        (note1: The support of this settings should have 'Support History Table' enabled in advance. Seeing [Slowly Changing Dimension](../../modeling/model_design/slowly_changing_dimension.md)

        (note2: Join relationship >= and < must be used in pairs, and same column must be joint in both conditions)

        - `foreign_key` - `string`, foreign key
        - `primary_key` - `string`, primary key
        - `op` - `string`, join relationship: LESS_THAN,GREATER_THAN_OR_EQUAL
    - `kind` -  `optional` `string`, table kind: FACT,LOOKUP, default: LOOKUP
    - `alias` -  `optional` `string`, alias
    - `flattenable` - `required` `string`, precomputing associations(precomputing: flatten, no-precomputing: normalized), flatten is suggested
    - `join_relation_type` -  `optional` `string`, join type: MANY_TO_ONE,MANY_TO_MANY, default: MANY_TO_ONE
  - `partition_desc` -  `optional` `JSON Object`, partition columns info
    - `partition_date_column` - `required` `string`, partition date column, format: TABLE.COLUMN
    - `partition_date_format` - `required` `string`, partition date column format, including: yyyy-MM-dd, yyyyMMdd... Supported date format please check "[Model Design Basics](../../modeling/data_modeling.md)"
    - `partition_type` -  `optional` `string`, partition type, including: APPEND, default: APPEND
  - `owner` -  `optional` `string`, the owner of model, default current user
  - `description` -  `optional` `string`, model description
  - `capacity` -  `optional` `string`, model capacity, including: SMALL,MEDIUM,LARGE, default: MEDIUM
  - `filter_condition` -  `optional` `string`, data filter condition. Data filter condition is an additional data filter during data loading.
  - `with_base_index` -  `optional` `boolean`, adding base indexes or not. Base indexes include all dimensions and measures of the model and automatically update as the model changes by default. default: false

- Curl Request Example

  ```sh
  curl -X POST \
    'http://host:port/kylin/api/models' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
  
    ### Stringify the following JSON Object when use
  
    -d '{
      "project": "pj01",
      "fact_table": "SSB.P_LINEORDER",
      "alias": "test_model_01",
      "management_type": "MODEL_BASED",
      "simplified_measures": [
          {
              "name": "COUNT_ALL",
              "expression": "COUNT",
              "parameter_value": [
                  {
                      "type": "constant",
                      "value": 1
                  }
              ],
              "return_type": ""
          }
      ],
      "simplified_dimensions": [
          {
              "column": "P_LINEORDER.LO_ORDERKEY",
              "name": "LO_ORDERKEY",
              "status": "DIMENSION"
          },
          {
              "column": "P_LINEORDER.LO_CUSTKEY",
              "name": "LO_CUSTKEY",
              "status": "DIMENSION"
          },
          {
              "column": "P_LINEORDER.LO_ORDERDATE",
              "name": "LO_ORDERDATE",
              "status": "DIMENSION"
          },
          {
              "column": "P_LINEORDER.LO_ORDERPRIOTITY",
              "name": "LO_ORDERPRIOTITY",
              "status": "DIMENSION"
          },
          {
              "column": "P_LINEORDER.V_REVENUE",
              "name": "V_REVENUE",
              "status": "DIMENSION"
          },
          {
              "column": "DATES.D_YEAR",
              "name": "D_YEAR",
              "status": "DIMENSION"
          },
          {
              "column": "CUSTOMER.C_NAME",
              "name": "C_NAME",
              "status": "DIMENSION"
          },
          {
              "column": "CUSTOMER.C_PHONE",
              "name": "C_PHONE",
              "status": "DIMENSION"
          }
      ],
      "join_tables": [
          {
              "table": "SSB.DATES",
              "join": {
                  "type": "LEFT",
                  "foreign_key": [
                      "P_LINEORDER.LO_ORDERDATE"
                  ],
                  "primary_key": [
                      "DATES.D_DATE"
                  ]
              },
              "alias": "DATES",
              "flattenable": "flatten",
              "join_relation_type": "MANY_TO_ONE"
          },
          {
              "table": "SSB.CUSTOMER",
              "join": {
                  "type": "LEFT",
                  "foreign_key": [
                      "P_LINEORDER.LO_CUSTKEY"
                  ],
                  "primary_key": [
                      "CUSTOMER.C_CUSTKEY"
                  ]
              },
              "alias": "CUSTOMER",
              "flattenable": "flatten",
              "join_relation_type": "MANY_TO_ONE"
          }
      ],
      "with_base_index": true
    }'
  ```

- Response Details

  - `code` - `string`, response code, succeed: `000`, failed: `999`
  - `msg` - `string`, response message
  - `data` - `JSON Object`, response data
    - `base_table_index` - `JSON Object`, base index info
      - `dimension_count` - `int`, dimension count
      - `measure_count` - `int`, measure count
      - `layout_id` - `long`, layout id
      - `operate_type` - `string`, operation type, including: UPDATE,CREATE
    - `base_agg_index` - `JSON Object`, base aggregation index info, same structure as `base_table_index`
    - `warn_code` - `string`, warning code message

- Response Example

  ```json
  {
    "code": "000",
    "msg": "",
    "data": {
      "base_table_index": {
          "dimension_count": 8,
          "measure_count": 0,
          "layout_id": 20000000001,
          "operate_type": "CREATE"
      },
      "base_agg_index": {
          "dimension_count": 8,
          "measure_count": 1,
          "layout_id": 1,
          "operate_type": "CREATE"
      },
      "warn_code": null
    }
  }
  ```

- Failed Response Example

  ```json
  {
    "code": "999",
    "data": null,
    "msg": "KE-010001002(Empty Project Name):Can’t find project information. Please select a project.",
    "stacktrace": "KE-010001002(Empty Project Name) \norg.apache.kylin.common.exception.KylinException: KE-010001002(Empty Project Name):Can’t find project information. ...",
    "exception": "KE-010001002(Empty Project Name):Can’t find project information. Please select a project.",
    "url": "http://host:port/kylin/api/models"
  }
  ```

- Error Code (Specific error message please check real api return.)

  - `KE-010001001`: Project Not Exist
  - `KE-010001002`: Empty Project Name
  - `KE-010006002`: Invalid Partition Column
  - `KE-010000003`: Invalid Parameter
  - `KE-010000002`: Invalid Range
  - `KE-010000004`: Invalid Name
  - `KE-010006005`: Timestamp Column Not Exist
  - `KE-010002010`: Failed to Add Models
  - `KE-010011001`: Duplicate Computed Column Name
  - `KE-010007001`: Table Not Exist



### Get Model List {#Get-Model-List}

> Call this API to get a list of models under specified project that satisfies certain conditions.

- `GET http://host:port/kylin/api/models`

- URL Parameters

  - `project` - `required`, `string`, project name.
  - `page_offset` - `optional`, `int`, offset of returned result, `0` by default.
  - `page_size` - `optional`, `int`, quantity of returned result per page, `10` by default.
  - `status` - `optional`, `string`, model status.
  - `model_name` - `optional`, `string`, model name.
  - `exact` - `optional`, `boolean`, whether exactly match the model name,  `true` by default.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/models?project=doc_expert' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Details

  - `Code`: `String`, response code. **Returned value**: `000` (request processing success), `999 ` (request processing failed)
  - `data`: `JSON`, returned data
    - `value`: details of the returned data, which consists of:
      - `name`: `String`, model name
      - `lookups`: `JSON`, detailed list of all dimension tables
      - `size_kb`: `Long`, total size of all segments of the models
      - `input_records_count`: `Long`, number of flattened tables within all segments
      - `input_records_size`: `Long`, size of flattened tables within all segments
      - `project`: `String`, project name
      - `uuid`: `String`, model ID
      - `last_modified`: `Long`, last modified time of the model
      - `create_time`: `Long`, model creation time
      - `mvcc`: `Long`, version number with metadata modified
      - `alias`: `String`, model alias
      - `owner`: `String`, model owner
      - `config_last_modifier`: `String`, last user who modified the configuration
      - `fact_table`: `String`, fact table (one model contains only one fact table)
      - `fact_table_alias`: `String`, fact table alias
      - `join_tables`: `JSON`, joined tables
      - `partition_desc`:`JSON`, partition column
      - `all_measures`: `JSON`, all measures within the model
      - `multi_partition_desc`: `JSON`, multiple partitions
      - `computed_columns`: `JSON`, computed columns
      - `canvas`: `JSON`, position of model canvas
      - `status`: `String`, model status; **Returned value**: `online`, `offline`, `broken`, `warning`
      - `last_build_end`: `String`, building time of the last segment
      - `storage`: `Long`, total storage size of the model; **Unit:** byte
      - `source`: `Long`, sourceByte sum of all segments in the model
      - `expansion_rate`: `String`, model expansion rate; **Unit:** %
      - `usage`: `Long`, queried times of the model
      - `model_broken`: `Boolean`, if the model is broken or not
      - `root_fact_table_deleted`: `Boolean`, if the fact table is deleted
      - `segments`: `JSON`, segment lists
      - `recommendations_count`: `Integer`, number of recommendations
      - `simplified_measures`: `JSON`, measure list of the models
      - `simplified_dimensions`: `JSON`, dimension list of the models
      - `simplified_tables`: `JSON`, all tables in the model
  - `offset`: page number
  - `limit`: models listed in each page
  - `total_size`: total number of models

- Response Example

  > [!NOTE]
  >
  > For parameters not listed in this page, you may go to the [Model Concepts and Operations](../../modeling/model_concepts_operations.md) for details.

  ```json
    {
        "code":"000",
        "data":{
            "value":[
                {
                    "name":"Model_03",
                    "lookups":Array[1],
                    "is_streaming":false,
                    "size_kb":0,
                    "input_records_count":0,
                    "input_records_size":0,
                    "project":null,
                    "uuid":"0464241b-fd7d-49a9-a3c9-b4f7e32cf489",
                    "last_modified":1574750372949,
                    "create_time":1574761225505,
                    "version":"4.0.0.0",
                    "mvcc":4,
                    "alias":"Model_03",
                    "owner":"ADMIN",
                    "config_last_modifier":null,
                    "config_last_modified":0,
                    "description":"",
                    "fact_table":"SSB.LINEORDER",
                    "fact_table_alias":null,
                    "management_type":"MODEL_BASED",
                    "join_tables":Array[1],
                    "filter_condition":"",
                    "partition_desc":Object{...},
                    "capacity":"MEDIUM",
                    "segment_config":Object{...},
                    "data_check_desc":null,
                    "semantic_version":0,
                    "all_named_columns":Array[26],
                    "all_measures":Array[2],
                    "column_correlations":Array[0],
                    "multilevel_partition_cols":Array[0],
                    "computed_columns":Array[0],
                    "canvas":Object{...},
                    "status":"ONLINE",
                    "last_build_end":"902016000000",
                    "storage":24694,
                    "source":5585164,
                    "expansion_rate":"0.44",
                    "usage":0,
                    "model_broken":false,
                    "root_fact_table_deleted":false,
                    "segments":null,
                    "recommendations_count":0,
                    "simplified_measures":Array[2],
                    "simplified_dimensions":Array[9],
                    "simplified_tables":Array[2],
                    "multi_partition_desc": {
                      "columns": ["KYLIN_SALES.LSTG_SITE_ID"],
                      "partitions": [
                          {
                              "id": 0,
                              "values": [
                                  "1"
                              ]
                          },
                          {
                              "id": 1,
                              "values": [
                                  "2"
                              ]
                          }
                      ],
                      "max_partition_id": 1
                    }
                },
                Object{...},
                Object{...}
            ],
            "offset":0,
            "limit":10,
            "total_size":3
        },
        "msg":""
    }
  ```

### Get Model Description {#Get-Model-Description}

> Call this API to get the descriptions of a model, for example, dimension, measure, fact table, and table join relations.
>

- `GET http://host:port/kylin/api/models/{project}/{model_name}/model_desc`

  > The API response does not contain dimensions and indexes recommended by the system.

- URL Parameters

  - `project` - `required` `string`, project name.
  - `model_name` - `required` `string`, model name.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/models/Kylin/AUTO_MODEL/model_desc'\
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Details

  - `code`: `String`, response code. **Returned value**: `000` (request processing success), `999 ` (request processing failed)
    - `data`: `JSON`, returned data
      - `uuid`: `String`, model ID
      - `last_modified`: `Long`, last modified time of the model
      - `name`: `String`, model name
      - `project`: `String`, project name
      - `dimensions`: `JSON`, dimension information
      - `id`: `Integer`, dimension ID
      - `name`: `String`, dimension name
      - `column`: `String`, column name
      - `status`: `String`, status
      - `derived`: `Boolean`, if the column is derived dimension
      - `measures`: `JSON`, measure information
      - `name`: `String`, measure name
      - `function`: `JSON`, functions
        - `expression`: `String`, expressions
        - `parameters`: `JSON`, parameters
          - `type`: `String`, parameter type
          - `value`: `String`, parameter value
      - `returntype`: `String`, returned type
      - `id`: `Integer`, measure ID
      - `aggregation_groups`: `JSON`, aggregation groups
      - `includes`: `JSON`, columns in aggregation groups
      - `select_rule`: `JSON`, type of aggregation groups
        - `hierarchy_dims`: `JSON`, dimension hierarchy
        - `mandatory_dims`: `JSON`, required hierarchy
        - `joint_dims`:  `JSON`, joint dimensions
        - `computed_columns`: `JSON`, [Computed Column](../../modeling/model_design/computed_column.md)
        - `tableIdentity`: `String`, table ID
        - `tableAlias`: `String`, table alias
        - `columnName`: `String`, column name
        - `expression`: `String`, expression of computed columns
        - `datatype`: `String`, column type
        - `comment`: `String`, notes of the columns
        - `rec_uuid`: `String`, primary key of the column
        - `fact_table`: `String`, fact table
        - `join_tables`: `JSON`, joined tables
          - `table`: `String`, table name
          - `kind`: `String`, table type; **Returned Value**: `FACT`, `LOOKUP`
          - `alias`: `String`, table alias
          - `join`: `String`, join relation
            - `type`: `String`, join type; **Returned Value**: `INNER`, `LEFT`, `RIGHT`, `OUTER`
            - `primary_key`: `JSON`, column that is a reference to other tables **Data Type**: JSON
            - `foreign_key`: `JSON`, columns in table that are joined with this table
            - `non_equi_join_condition`: `JSON`, non-equi join conditions (object)
            - `primary_table`: `String`, current table
            - `foreign_table`: `String`, tables joined to current table
          - `flattenable`: `String`, if dimension tables are flattened tables, **Data Type**: String
        - `join_relation_type`: `String`, join relations; **Returned Value**: `MANY_TO_ONE`, `ONE_TO_ONE`, `ONE_TO_MANY`, `MANY_TO_MANY`

- Response Example

  ```json
   {
      "code":"000",
      "data":{
          "uuid":"24dc8eae-e613-40ce-b601-26f065f24070",
          "last_modified":1577436020423,
          "version":"4.0.0.0",
          "name":"Kylin",
          "project":"test_gy",
          "description":"",
          "dimensions":[
              {
                  "id":8,
                  "name":"P_LINEORDER_LO_CUSTKEY",
                  "column":"P_LINEORDER.LO_CUSTKEY",
                  "status":"DIMENSION",
                  "derived":false
              }
          ],
          "measures":[
              {
                  "name":"COUNT_ALL",
                  "function":{
                      "expression":"COUNT",
                      "parameters":[
                          {
                              "type":"constant",
                              "value":"1"
                          }
                      ],
                      "returntype":"bigint"
                  },
                  "id":100000
              },
              {
                  "name":"TEST",
                  "function":{
                      "expression":"SUM",
                      "parameters":[
                          {
                              "type":"column",
                              "value":"P_LINEORDER.LO_QUANTITY"
                          }
                      ],
                      "returntype":"bigint"
                  },
                  "id":100001
              }
          ],
          "aggregation_groups":[
              {
                  "includes":[
                      "P_LINEORDER.LO_CUSTKEY"
                  ],
                  "select_rule":{
                      "hierarchy_dims":[
  
                      ],
                      "mandatory_dims":[
  
                      ],
                      "joint_dims":[
  
                      ]
                  }
              }
          ],
          "computed_columns": [{
            "tableIdentity": "SSB.P_LINEORDER",
            "tableAlias": "P_LINEORDER",
            "columnName": "CASTCOL",
            "expression": "CAST(P_LINEORDER.LO_PARTKEY as bigint)",
            "innerExpression": "CAST(`P_LINEORDER`.`LO_PARTKEY` as bigint)",
            "datatype": "BIGINT",
            "comment": null,
            "rec_uuid": null
          },{...}],
          "fact_table": "SSB.P_LINEORDER",
          "join_tables": [{
            "table": "SSB.CUSTOMER",
            "kind": "LOOKUP",
            "alias": "CUSTOMER",
            "join": {
                "type": "INNER",
                "primary_key": [
                    "CUSTOMER.C_CUSTKEY"
                ],
                "foreign_key": [
                    "P_LINEORDER.LO_CUSTKEY"
                ],
                "non_equi_join_condition": null,
                "primary_table": null,
                "foreign_table": null
            },
            "flattenable": "flatten",
            "join_relation_type": "MANY_TO_ONE"
          },{...}]
      },
      "msg":""
  }
  ```

### Define Partition Column{#Define Partition Colum}

> Call this API to set partition columns for certain model of certain project. To enable incremental building for a model, you should first define a partition column for this model. After this operation, Kylin will complete incremental building.

- `PUT http://host:port/kylin/api/models/{model_name}/partition`

- URL Parameters

  - `model_name` - `required` `string`, model name.

- HTTP Body: JSON Object
  - `project` - `required` `string`，project name.

  - `partition_desc` - `optional`， partition colunm information
    - `partition_date_column` - `optional` `string`, partition colunm
    - `partition_date_format` - `optional` `string`，partition format

  - `multi_partition_desc` - `optional`, multi-level partitioning model sub-partition colunm information
    - `columns` - `array[string]`, multi-level partitioning model sub-partition colunm

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X PUT \
    http://host:port/kylin/api/models/multi_partition_model/partition \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json' \
    -d '{
      "project": "multi_partition",
      "partition_desc": {
          "partition_date_column": "KYLIN_SALES.PART_DT",
          "partition_date_format": "yyyy-MM-dd"
      },
      "multi_partition_desc": {
          "columns": [
              "KYLIN_SALES.LSTG_SITE_ID"
          ]
      }
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

### Get index list{#get-index-list}

> Get indexes of given model.
>

+ `GET http://host:port/kylin/api/models/{model_name}/indexes`

+ URL Parameters

  + `project` - `required` `string`, project name
  + `model_name` - `required ` `string`，model alias
  + `status` - `optional` `string`, index status，support `NO_BUILD`, `BUILDING`, `LOCKED`, `ONLINE`, empty by default
  + `page_offset` - `optional` `int`, offset of returned result, `0` by default.
  + `page_size` - `optional` `int`, quantity of returned result per page, `10` by default.
  + `sources` - `optional` `string`, type of index，support `RECOMMENDED_AGG_INDEX`, `RECOMMENDED_TABLE_INDEX`, `CUSTOM_AGG_INDEX`, `CUSTOM_TABLE_INDEX`, empty by default
  + `sort_by` - `optional` `string`, sort by field，support `last_modified`, `usage`, `data_size`, `last_modified` by default
  + `reverse` - `optional` `boolean`, whether sort reverse, `true` by default
  + `batch_index_ids` - `optional` `array[long]`,specify indexId, empty by default

+ HTTP Header

  + `Accept: application/vnd.apache.kylin-v4-public+json`
  + `Accept-Language: en`
  + `Content-Type: application/json;charset=utf-8`

+ Curl Request Example

  ```shell
  curl -X GET 'http://host:port/kylin/api/models/m1/indexes?project=ssb&batch_index_ids=1,10001,20001' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

* Response Details

  * `absent_batch_index_ids`, not found indexId
  * `indexes`, index list
    * `related_tables`, index related table

* Response Example

  ```json
  {
      "code": "000",
      "data": {
          "project": "ssb",
          "uuid": "383cd655-89f4-4986-85cf-1d299e287e84",
          "model_name": "m1",
          "total_size": 2,
          "offset": 0,
          "limit": 10,
          "owner": "ADMIN",
          "absent_batch_index_ids": [20001],
          "indexes": [
              {
                  "id": 1,
                  "status": "NO_BUILD",
                  "source": "CUSTOM_AGG_INDEX",
                  "col_order": [
                      {
                          "key": "LINEORDER.LO_ORDERKEY",
                          "value": "column",
                          "cardinality": null
                      },
                      {
                          "key": "LINEORDER.LO_LINENUMBER",
                          "value": "column",
                          "cardinality": null
                      },
                      {
                          "key": "COUNT_ALL",
                          "value": "measure",
                          "cardinality": null
                      }
                  ],
                  "shard_by_columns": [],
                  "sort_by_columns": [],
                  "data_size": 0,
                  "usage": 0,
                  "last_modified": 1610462519405,
                  "related_tables": ["SSB.LINEORDER"],
                  "storage_type": 20
              },
              {
                  "id": 10001,
                  "status": "NO_BUILD",
                  "source": "CUSTOM_AGG_INDEX",
                  "col_order": [
                      {
                          "key": "LINEORDER.LO_ORDERKEY",
                          "value": "column",
                          "cardinality": null
                      },
                      {
                          "key": "COUNT_ALL",
                          "value": "measure",
                          "cardinality": null
                      }
                  ],
                  "shard_by_columns": [],
                  "sort_by_columns": [],
                  "data_size": 0,
                  "usage": 0,
                  "last_modified": 1610462519405,
                  "related_tables": ["SSB.LINEORDER"],
                  "storage_type": 20
              }
          ]
      },
      "msg": ""
  }
  ```

### Delete Model {#Delete_Model}

- `DELETE  http://host:port/kylin/api/models/{model_name}`

- Request Permission: MANAGEMENT permission and above

- Introduced in: 5.0

- URL Parameters

  - `model_name` - `required` `string`, model name

- Request Parameters

  - `project` - `required` `string`, project name

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X DELETE \
  'http://host:port/kylin/api/models/{model_name}?project=test' \
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

### Export TDS File (BETA) {#EXPORT_TDS} 
- `GET http://host:port/kylin/api/models/{model_name}/export`
- URL Parameters

  - `model_name` - `required` `string`, the model name
  - `project` - `required` `string`, the project name
  - `export_as` - `required` `string`, the format to export to
    - `TABLEAU_ODBC_TDS` export as Tableau TDS with Kyligence ODBC datasource
    - `TABLEAU_CONNECTOR_TDS` export as Tableau TDS with Kyligence Tableau Connector as the datasource
  - `element` - `string` `optional` the model elements to export
    - `AGG_INDEX_COL` (default) export only dimensions and meausres defined in Aggregate Groups
    - `AGG_INDEX_AND_TABLE_INDEX_COL` export only dimensions and meausres defined in Aggregate Groups or Table Indexes
    - `ALL_COLS` export all dimensions and measures defined in the model
  - `server_host` - `string` `optional`, the host name of the exported Kylin datasource, default to the current requesting host
  - `server_port` - `string` `optional`, the port of the export Kylin datasource, default to the current requesting port
- HTTP Header

  - Accept:application/vnd.apache.kylin-v4-public+json
  - Accept-Language: en
  - Content-Type: application/json;charset=utf-8
- Curl Request Example

```sh
curl -X GET \
  'http://host:port/kylin/api/models/a3/export?project=test_project&export_as=TABLEAU_ODBC_TDS&element=AGG_INDEX_COL&server_host=host&server_port=7080' \
  -H 'accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \

```

### Model Rename {#Model_Rename}

- `PUT http://host:port/kylin/api/models/{model_name}/name`

- Introduced in: 5.0

- URL Parameters

  - `model_name` - `required` `string`，model name。

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: cn`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object

  - `project_name` - `required` `string`, project name.
  - `new_model_name` - `required` `string`, the new name of the model.

- Curl Request Example

  ```sh
  curl -X PUT \
  'http://host:port/kylin/api/models/test_model/name' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: cn' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -d '{"project":"ssb", "new_model_name":"testNewName"}'
  ```

- Response Example

  ```json
  {
      "code": "000",
      "data": "",
      "msg": ""
  }
  ```



### Model Online/Offline {#Model_Online_Offline}

- `PUT http://host:port/kylin/api/models/{model_name}/status`

- Request Permission: MANAGEMENT permission and above

- Introduced in: 5.0

- URL Parameters

  - `model_name` - `required` `string`, model name

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- HTTP Body: JSON Object

  - project - `required` `string`, project name
  - status - `required` `string`, update model status, including: ONLINE,OFFLINE

- Curl Request Example

  ```sh
  curl -X PUT \
  'http://host:port/kylin/api/models/model_test1/status' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8'
  -d '{"project": "pj01", "status": "OFFLINE"}'
  ```

- Response Example

  ```json
  {
      "code": "000",
      "data": "",
      "msg": ""
  }
  ```

- Failed Response Example

  ```json
  {
    "code": "999",
    "data": null,
    "msg": "KE-010001002(Empty Project Name):Can’t find project information. Please select a project.",
    "stacktrace": "KE-010001002(Empty Project Name) \norg.apache.kylin.common.exception.KylinException: KE-010001002(Empty Project Name):Can’t find project information. ...",
    "exception": "KE-010001002(Empty Project Name):Can’t find project information. Please select a project.",
    "url": "http://host:port/kylin/api/models"
  }
  ```

- Error Code (Specific error message please check real api return.)

  - `KE-010001001`: Project Not Exist
  - `KE-010001002`: Empty Project Name
  - `KE-010000003`: Invalid Parameter

### Export TDS File based on user rights (BETA) {#Export_TDS_File_based_on_user_rights}

- `GET http://host:port/kylin/api/models/bi_export`
- URL Parameters

  - `model_name` - `required` `string`, the model name
  - `project` - `required` `string`, the project name
  - `export_as` - `required` `string`, the format to export to
    - `TABLEAU_ODBC_TDS` export as Tableau TDS with Kyligence ODBC datasource
    - `TABLEAU_CONNECTOR_TDS` export as Tableau TDS with Kyligence Tableau Connector as the datasource
  - `element` - `string` `optional` the model elements to export
    - `AGG_INDEX_COL` (default) export only dimensions and meausres defined in Aggregate Groups based on user rights
    - `AGG_INDEX_AND_TABLE_INDEX_COL` export only dimensions and meausres defined in Aggregate Groups or Table Indexes based on user rights
    - `ALL_COLS` export all dimensions and measures defined in the model based on user rights
    - `CUSTOM_COLS` export custom dimensions and measures defined in the model based on user rights, When using this method, the `dimensions`, ` measures` parameter must be add
  - `server_host` - `string` `optional`, the host name of the exported Kylin datasource, default to the current requesting host
  - `server_port` - `string` `optional`, the port of the export Kylin datasource, default to the current requesting port
  - `dimensions` - `List<String>`  `optional`, An exported dimension list in the format of `${table_alias}.${columnName1},${table_alias}.${columnName2} `, Parameter takes effect of `element=CUSTOM_COLS`
  - `measures` - `List<String>`  `optional`, An exported measures list in the format of  `${measureName1},${measureName2`, Parameter takes effect of `element=CUSTOM_COLS`
  
- HTTP Header

  - `Accept:application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`
- Curl Request Example

```sh
curl -X GET \
  'http://{host}:{port}/kylin/api/models/bi_export?model_name={modelName}&project={project}&export_as=TABLEAU_ODBC_TDS&server_host=localhost&server_port=8080&dimensions=CUSTOMER.C_NAME,CUSTOMER.CC_6,CUSTOMER.CC_7,CUSTOMER.CC_9,LINEORDER_1.LO_CUSTKEY&measures=m_aa&element=CUSTOM_COLS' \
  -H 'accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'authorization: Basic QURNSU46S1lMSU4=' \
```

- Failed Response Example(When all columns in the model have no permissions or table-joined columns have no permissions)

```json
{
    "code": "999",
    "data": null,
    "msg": "KE-010002022(The table not contains unauthenticated columns):Please add permissions to columns in the table!",
    "stacktrace": "KE-010002022(The table not contains unauthenticated columns) \norg.apache.kylin.common.exception.KylinException: KE-010002022(The table not contains unauthenticated columns):Please add permissions to columns in the table!8)...。",
    "exception": "KE-010002022(The table not contains unauthenticated columns):Please add permissions to columns in the table!",
    "url": "http://host:port/kylin/api/models/bi_export"
}
```

- Error Code (Specific error message please check real api return.)
  - `KE-010002022`: Please add permissions to columns in the table
- Successful Code (exported as TDS file contents)

```xml
<?xml version='1.0' encoding='UTF-8'?>
<datasource formatted-name="federated.0e6gjbn18cj0a41an9pi309itkyi" inline="true" source-platform="win" version="10.0">
    <connection class="federated">
        <named-connections>
            <named-connection caption="localhost" name="genericodbc.11du78x0szfyb51b703es1ocv315">
                <connection class="genericodbc" dbname="" odbc-connect-string-extras="PROJECT=KE_36166;CUBE=test_model_3_cc" odbc-dbms-name="MySQL" odbc-driver="KyligenceODBCDriver" odbc-dsn="" odbc-suppress-connection-pooling="" odbc-use-connection-pooling="" port="8080" schema="DEFAULT" server="localhost" username="ADMIN"/>
            </named-connection>
        </named-connections>
        <relation join="inner" type="join">
            <clause type="join">
                <expression op="=">
                    <expression op="[CUSTOMER].[C_CUSTKEY]"/>
                    <expression op="[CUSTOMER_1].[C_CUSTKEY]"/></expression>
            </clause>
            <relation join="inner" type="join">
                <clause type="join">
                    <expression op="=">
                        <expression op="[CUSTOMER].[C_CUSTKEY]"/>
                        <expression op="[LINEORDER_1].[LO_CUSTKEY]"/></expression>
                </clause>
                <relation type="table" connection="genericodbc.11du78x0szfyb51b703es1ocv315" name="CUSTOMER" table="[SSB].[CUSTOMER]"/>
                <relation type="table" connection="genericodbc.11du78x0szfyb51b703es1ocv315" name="LINEORDER_1" table="[SSB].[LINEORDER]"/></relation>
            <relation type="table" connection="genericodbc.11du78x0szfyb51b703es1ocv315" name="CUSTOMER_1" table="[SSB1].[CUSTOMER]"/></relation>
        <cols>
            <map key="[LO_ORDERPRIOTITY]" value="[LINEORDER_1].[LO_ORDERPRIOTITY]"/>
            <map key="[C_ADDRESS (CUSTOMER_1)]" value="[CUSTOMER_1].[C_ADDRESS]"/>
            <map key="[C_NAME (CUSTOMER_1)]" value="[CUSTOMER_1].[C_NAME]"/>
            <map key="[C_REGION (CUSTOMER_1)]" value="[CUSTOMER_1].[C_REGION]"/>
            <map key="[C_NATION (CUSTOMER)]" value="[CUSTOMER].[C_NATION]"/>
            <map key="[C_CUSTKEY (CUSTOMER)]" value="[CUSTOMER].[C_CUSTKEY]"/>
            <map key="[C_MKTSEGMENT (CUSTOMER)]" value="[CUSTOMER].[C_MKTSEGMENT]"/>
            <map key="[LO_PARTKEY]" value="[LINEORDER_1].[LO_PARTKEY]"/>
            <map key="[C_CITY (CUSTOMER)]" value="[CUSTOMER].[C_CITY]"/>
            <map key="[LO_LINENUMBER]" value="[LINEORDER_1].[LO_LINENUMBER]"/>
            <map key="[C_REGION (CUSTOMER)]" value="[CUSTOMER].[C_REGION]"/>
            <map key="[LO_ORDERKEY]" value="[LINEORDER_1].[LO_ORDERKEY]"/>
            <map key="[C_PHONE (CUSTOMER)]" value="[CUSTOMER].[C_PHONE]"/>
            <map key="[CC_10]" value="[CUSTOMER].[CC_10]"/>
            <map key="[C_ADDRESS (CUSTOMER)]" value="[CUSTOMER].[C_ADDRESS]"/>
            <map key="[CC_4]" value="[CUSTOMER].[CC_4]"/>
            <map key="[LO_SHIPMODE]" value="[LINEORDER_1].[LO_SHIPMODE]"/>
            <map key="[C_CITY (CUSTOMER_1)]" value="[CUSTOMER_1].[C_CITY]"/>
            <map key="[CC_6]" value="[CUSTOMER].[CC_6]"/>
            <map key="[C_NATION (CUSTOMER_1)]" value="[CUSTOMER_1].[C_NATION]"/>
            <map key="[CC_5]" value="[CUSTOMER].[CC_5]"/>
            <map key="[C_PHONE (CUSTOMER_1)]" value="[CUSTOMER_1].[C_PHONE]"/>
            <map key="[CC_7]" value="[CUSTOMER].[CC_7]"/>
            <map key="[CC_9]" value="[CUSTOMER].[CC_9]"/>
            <map key="[LO_QUANTITY]" value="[LINEORDER_1].[LO_QUANTITY]"/>
            <map key="[LO_SUPPLYCOST]" value="[LINEORDER_1].[LO_SUPPLYCOST]"/>
            <map key="[C_CUSTKEY (CUSTOMER_1)]" value="[CUSTOMER_1].[C_CUSTKEY]"/>
            <map key="[LO_CUSTKEY]" value="[LINEORDER_1].[LO_CUSTKEY]"/>
            <map key="[LO_ORDTOTALPRICE]" value="[LINEORDER_1].[LO_ORDTOTALPRICE]"/>
            <map key="[C_NAME (CUSTOMER)]" value="[CUSTOMER].[C_NAME]"/>
            <map key="[LO_COMMITDATE]" value="[LINEORDER_1].[LO_COMMITDATE]"/>
            <map key="[LO_EXTENDEDPRICE]" value="[LINEORDER_1].[LO_EXTENDEDPRICE]"/>
            <map key="[LO_REVENUE]" value="[LINEORDER_1].[LO_REVENUE]"/>
            <map key="[LO_DISCOUNT]" value="[LINEORDER_1].[LO_DISCOUNT]"/>
            <map key="[LO_SHIPPRIOTITY]" value="[LINEORDER_1].[LO_SHIPPRIOTITY]"/>
            <map key="[LO_SUPPKEY]" value="[LINEORDER_1].[LO_SUPPKEY]"/>
            <map key="[LO_TAX]" value="[LINEORDER_1].[LO_TAX]"/>
            <map key="[LO_ORDERDATE]" value="[LINEORDER_1].[LO_ORDERDATE]"/>
            <map key="[C_MKTSEGMENT (CUSTOMER_1)]" value="[CUSTOMER_1].[C_MKTSEGMENT]"/>
        </cols>
    </connection>
    <aliases enabled="yes"/>
    <column caption="LO_ORDERPRIOTITY" datatype="string" name="[LO_ORDERPRIOTITY]" role="dimension" type="nominal" hidden="true"/>
    <column caption="C_ADDRESS_CUSTOMER_1" datatype="string" name="[C_ADDRESS (CUSTOMER_1)]" role="dimension" type="nominal" hidden="true"/>
    <column caption="C_NAME_CUSTOMER_1" datatype="string" name="[C_NAME (CUSTOMER_1)]" role="dimension" type="nominal" hidden="true"/>
    <column caption="C_REGION_CUSTOMER_1" datatype="string" name="[C_REGION (CUSTOMER_1)]" role="dimension" type="nominal" hidden="true"/>
    <column caption="C_NATION" datatype="string" name="[C_NATION (CUSTOMER)]" role="dimension" type="nominal" hidden="true"/>
    <column caption="C_CUSTKEY" datatype="integer" name="[C_CUSTKEY (CUSTOMER)]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="C_MKTSEGMENT" datatype="string" name="[C_MKTSEGMENT (CUSTOMER)]" role="dimension" type="nominal" hidden="true"/>
    <column caption="LO_PARTKEY" datatype="integer" name="[LO_PARTKEY]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="C_CITY" datatype="string" name="[C_CITY (CUSTOMER)]" role="dimension" type="nominal" hidden="true"/>
    <column caption="LO_LINENUMBER" datatype="integer" name="[LO_LINENUMBER]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="C_REGION" datatype="string" name="[C_REGION (CUSTOMER)]" role="dimension" type="nominal" hidden="true"/>
    <column caption="LO_ORDERKEY" datatype="integer" name="[LO_ORDERKEY]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="C_PHONE" datatype="string" name="[C_PHONE (CUSTOMER)]" role="dimension" type="nominal" hidden="true"/>
    <column caption="CC_10" datatype="integer" name="[CC_10]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="C_ADDRESS" datatype="string" name="[C_ADDRESS (CUSTOMER)]" role="dimension" type="nominal" hidden="true"/>
    <column caption="CC_4" datatype="integer" name="[CC_4]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="LO_SHIPMODE" datatype="string" name="[LO_SHIPMODE]" role="dimension" type="nominal" hidden="true"/>
    <column caption="C_CITY_CUSTOMER_1" datatype="string" name="[C_CITY (CUSTOMER_1)]" role="dimension" type="nominal" hidden="true"/>
    <column caption="CC_6" datatype="integer" name="[CC_6]" role="dimension" type="ordinal"/>
    <column caption="C_NATION_CUSTOMER_1" datatype="string" name="[C_NATION (CUSTOMER_1)]" role="dimension" type="nominal" hidden="true"/>
    <column caption="CC_5" datatype="integer" name="[CC_5]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="C_PHONE_CUSTOMER_1" datatype="string" name="[C_PHONE (CUSTOMER_1)]" role="dimension" type="nominal" hidden="true"/>
    <column caption="CC_7" datatype="integer" name="[CC_7]" role="dimension" type="ordinal"/>
    <column caption="CC_9" datatype="integer" name="[CC_9]" role="dimension" type="ordinal"/>
    <column caption="LO_QUANTITY" datatype="integer" name="[LO_QUANTITY]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="LO_SUPPLYCOST" datatype="integer" name="[LO_SUPPLYCOST]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="C_CUSTKEY_CUSTOMER_1" datatype="integer" name="[C_CUSTKEY (CUSTOMER_1)]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="LO_CUSTKEY" datatype="integer" name="[LO_CUSTKEY]" role="dimension" type="ordinal"/>
    <column caption="LO_ORDTOTALPRICE" datatype="integer" name="[LO_ORDTOTALPRICE]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="C_NAME_1" datatype="string" name="[C_NAME (CUSTOMER)]" role="dimension" type="nominal"/>
    <column caption="LO_COMMITDATE" datatype="date" name="[LO_COMMITDATE]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="LO_EXTENDEDPRICE" datatype="integer" name="[LO_EXTENDEDPRICE]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="LO_REVENUE" datatype="integer" name="[LO_REVENUE]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="LO_DISCOUNT" datatype="integer" name="[LO_DISCOUNT]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="LO_SHIPPRIOTITY" datatype="integer" name="[LO_SHIPPRIOTITY]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="LO_SUPPKEY" datatype="integer" name="[LO_SUPPKEY]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="LO_TAX" datatype="integer" name="[LO_TAX]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="LO_ORDERDATE" datatype="date" name="[LO_ORDERDATE]" role="dimension" type="ordinal" hidden="true"/>
    <column caption="C_MKTSEGMENT_CUSTOMER_1" datatype="string" name="[C_MKTSEGMENT (CUSTOMER_1)]" role="dimension" type="nominal" hidden="true"/>
    <column caption="COUNT_ALL" datatype="integer" name="[COUNT_ALL]" role="measure" type="quantitative" hidden="true">
        <calculation class="tableau" formula="COUNT(1)"/>
    </column>
    <column caption="" datatype="integer" name="[m_aa]" role="measure" type="quantitative">
        <calculation class="tableau" formula="SUM([CC_4])"/>
    </column>
    <column caption="" datatype="integer" name="[m_bb]" role="measure" type="quantitative" hidden="true">
        <calculation class="tableau" formula="SUM([CC_5])"/>
    </column>
    <column caption="" datatype="integer" name="[m_cc]" role="measure" type="quantitative" hidden="true">
        <calculation class="tableau" formula="SUM([C_CUSTKEY (CUSTOMER)])"/>
    </column>
    <column caption="" datatype="integer" name="[m_dd]" role="measure" type="quantitative" hidden="true">
        <calculation class="tableau" formula="SUM([CC_10])"/>
    </column>
    <drill-paths>
        <drill-path name="[C_ADDRESS (CUSTOMER)], [C_NATION (CUSTOMER)], [CC_7]">
            <field>[C_ADDRESS (CUSTOMER)]</field>
            <field>[C_NATION (CUSTOMER)]</field>
            <field>[CC_7]</field>
        </drill-path>
    </drill-paths>
    <semantic-values>
        <semantic-value key="[Country].[Name]" value="&quot;美国&quot;"/>
    </semantic-values>
</datasource>
```

