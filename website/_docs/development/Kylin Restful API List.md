## Kylin Restful API List

* QUERY
   * Login
   * Query
   * List queryable tables
* CUBE
   * List cubes
   * Rebuild cube
   * Disable cube
   * Purge cube
   * Enable cube
* JOB
   * Resume job
   * Discard job
   * Get job step log
* Metadata
   * Get Hive Table
   * Get Hive Table (Extend Info)
   * Get Hive Tables 
   * Load Hive Table
* Wipe cache

***

### Login
`POST /user/authentication`

for example: 
```sh
curl -c /path/to/cookiefile.txt -X POST -H "Authorization: Basic XXXXXXXXX" -H 'Content-Type: application/json' http://<host>:<port>/kylin/api/user/authentication
```

If login successfully, the JSESSIONID will be saved into the cookie file; In the subsequent http requests, attach the cookie, for example:
```sh
curl -b /path/to/cookiefile.txt -X PUT -H 'Content-Type: application/json' -d '{"startTime":'1423526400000', "endTime":'1423526400', "buildType":"BUILD"}' http://<host>:<port>/kylin/api/cubes/your_cube/rebuild
```

#### Header
Authorization data encoded by basic auth. Header sample:
Authorization:Basic {data}
#### Response Sample
```sh
Status: 200OK
```
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

***

### Query
`POST /query`
#### Request Body
* sql - `required` `string` The sql query string.
* offset - `optional` `int` Query offset. If offset is set in sql, curIndex will be ignored.
* limit - `optional` `int` Query limit. If limit is set in sql, perPage will be ignored.
* acceptPartial - `optional` `bool` Flag to switch partial query.
* project - `optional` `string` Project to perform query. Default value is 'DEFAULT'
```sh
{  
   "sql":"select * from TEST_KYLIN_FACT",
   "offset":0,
   "limit":50000,
   "acceptPartial":true,
   "project":"DEFAULT"
}
```
#### Response Sample
```sh
Status: 200OK
```
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
      },
      {  
         "isNullable":1,
         "displaySize":10,
         "label":"LSTG_SITE_ID",
         "name":"LSTG_SITE_ID",
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
      },
      {  
         "isNullable":1,
         "displaySize":5,
         "label":"SLR_SEGMENT_CD",
         "name":"SLR_SEGMENT_CD",
         "schemaName":null,
         "catelogName":null,
         "tableName":null,
         "precision":5,
         "scale":0,
         "columnType":5,
         "columnTypeName":"SMALLINT",
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
         "displaySize":256,
         "label":"LSTG_FORMAT_NAME",
         "name":"LSTG_FORMAT_NAME",
         "schemaName":null,
         "catelogName":null,
         "tableName":null,
         "precision":256,
         "scale":0,
         "columnType":12,
         "columnTypeName":"VARCHAR(256)",
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
         "displaySize":19,
         "label":"SELLER_ID",
         "name":"SELLER_ID",
         "schemaName":null,
         "catelogName":null,
         "tableName":null,
         "precision":19,
         "scale":0,
         "columnType":-5,
         "columnTypeName":"BIGINT",
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
         "displaySize":19,
         "label":"PRICE",
         "name":"PRICE",
         "schemaName":null,
         "catelogName":null,
         "tableName":null,
         "precision":19,
         "scale":4,
         "columnType":3,
         "columnTypeName":"DECIMAL(19, 4)",
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
         "displaySize":19,
         "label":"MIN_PRICE_",
         "name":"MIN_PRICE_",
         "schemaName":null,
         "catelogName":null,
         "tableName":null,
         "precision":19,
         "scale":4,
         "columnType":3,
         "columnTypeName":"DECIMAL(19, 4)",
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
         "displaySize":19,
         "label":"MAX_PRICE_",
         "name":"MAX_PRICE_",
         "schemaName":null,
         "catelogName":null,
         "tableName":null,
         "precision":19,
         "scale":4,
         "columnType":3,
         "columnTypeName":"DECIMAL(19, 4)",
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
         "displaySize":19,
         "label":"COUNT__",
         "name":"COUNT__",
         "schemaName":null,
         "catelogName":null,
         "tableName":null,
         "precision":19,
         "scale":0,
         "columnType":-5,
         "columnTypeName":"BIGINT",
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
      ],
      [  
         "2013-08-07",
         "170083",
         "3",
         "12",
         "FP-non GTC",
         "10000688",
         "49.4518822857833",
         "49.4518822857833",
         "49.4518822857833",
         "1"
      ],
      [  
         "2013-08-07",
         "73506",
         "0",
         "14",
         "Others",
         "10000858",
         "35.7556514660872",
         "35.7556514660872",
         "35.7556514660872",
         "1"
      ],
      [  
         "2013-08-07",
         "26262",
         "0",
         "5",
         "FP-GTC",
         "10000914",
         "69.63202173718798",
         "69.63202173718798",
         "69.63202173718798",
         "1"
      ],
      [  
         "2013-08-07",
         "1161",
         "3",
         "12",
         "Others",
         "10000959",
         "76.72392839984632",
         "76.72392839984632",
         "76.72392839984632",
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

***

### List queryable tables
`GET /tables_and_columns`
#### Request Parameters
* project - `required` `string` The project to load tables

#### Response Sample
```sh
Status: 200OK
```
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
   },
   {  
      "columns":[  
         {  
            "table_NAME":"TEST_CATEGORY_GROUPINGS",
            "table_SCHEM":"EDW",
            "column_NAME":"LEAF_CATEG_ID",
            "data_TYPE":4,
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
            "type_NAME":"INTEGER"
         },
         {  
            "table_NAME":"TEST_CATEGORY_GROUPINGS",
            "table_SCHEM":"EDW",
            "column_NAME":"SITE_ID",
            "data_TYPE":4,
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
            "type_NAME":"INTEGER"
         },
         {  
            "table_NAME":"TEST_CATEGORY_GROUPINGS",
            "table_SCHEM":"EDW",
            "column_NAME":"META_CATEG_NAME",
            "data_TYPE":12,
            "nullable":1,
            "column_SIZE":256,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":256,
            "ordinal_POSITION":3,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"VARCHAR(256) CHARACTER SET \"UTF-16LE\" COLLATE \"UTF-16LE$en_US$primary\""
         },
         {  
            "table_NAME":"TEST_CATEGORY_GROUPINGS",
            "table_SCHEM":"EDW",
            "column_NAME":"CATEG_LVL2_NAME",
            "data_TYPE":12,
            "nullable":1,
            "column_SIZE":256,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":256,
            "ordinal_POSITION":4,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"VARCHAR(256) CHARACTER SET \"UTF-16LE\" COLLATE \"UTF-16LE$en_US$primary\""
         },
         {  
            "table_NAME":"TEST_CATEGORY_GROUPINGS",
            "table_SCHEM":"EDW",
            "column_NAME":"CATEG_LVL3_NAME",
            "data_TYPE":12,
            "nullable":1,
            "column_SIZE":256,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":256,
            "ordinal_POSITION":5,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"VARCHAR(256) CHARACTER SET \"UTF-16LE\" COLLATE \"UTF-16LE$en_US$primary\""
         },
         {  
            "table_NAME":"TEST_CATEGORY_GROUPINGS",
            "table_SCHEM":"EDW",
            "column_NAME":"USER_DEFINED_FIELD1",
            "data_TYPE":12,
            "nullable":1,
            "column_SIZE":256,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":256,
            "ordinal_POSITION":6,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"VARCHAR(256) CHARACTER SET \"UTF-16LE\" COLLATE \"UTF-16LE$en_US$primary\""
         },
         {  
            "table_NAME":"TEST_CATEGORY_GROUPINGS",
            "table_SCHEM":"EDW",
            "column_NAME":"USER_DEFINED_FIELD3",
            "data_TYPE":12,
            "nullable":1,
            "column_SIZE":256,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":256,
            "ordinal_POSITION":7,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"VARCHAR(256) CHARACTER SET \"UTF-16LE\" COLLATE \"UTF-16LE$en_US$primary\""
         },
         {  
            "table_NAME":"TEST_CATEGORY_GROUPINGS",
            "table_SCHEM":"EDW",
            "column_NAME":"UPD_DATE",
            "data_TYPE":12,
            "nullable":1,
            "column_SIZE":256,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":256,
            "ordinal_POSITION":8,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"VARCHAR(256) CHARACTER SET \"UTF-16LE\" COLLATE \"UTF-16LE$en_US$primary\""
         },
         {  
            "table_NAME":"TEST_CATEGORY_GROUPINGS",
            "table_SCHEM":"EDW",
            "column_NAME":"UPD_USER",
            "data_TYPE":12,
            "nullable":1,
            "column_SIZE":256,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":256,
            "ordinal_POSITION":9,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"VARCHAR(256) CHARACTER SET \"UTF-16LE\" COLLATE \"UTF-16LE$en_US$primary\""
         }
      ],
      "table_NAME":"TEST_CATEGORY_GROUPINGS",
      "table_SCHEM":"EDW",
      "ref_GENERATION":null,
      "self_REFERENCING_COL_NAME":null,
      "type_SCHEM":null,
      "table_TYPE":"TABLE",
      "table_CAT":"defaultCatalog",
      "remarks":null,
      "type_CAT":null,
      "type_NAME":null
   },
   {  
      "columns":[  
         {  
            "table_NAME":"TEST_KYLIN_FACT",
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
            "table_NAME":"TEST_KYLIN_FACT",
            "table_SCHEM":"EDW",
            "column_NAME":"LEAF_CATEG_ID",
            "data_TYPE":4,
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
            "type_NAME":"INTEGER"
         },
         {  
            "table_NAME":"TEST_KYLIN_FACT",
            "table_SCHEM":"EDW",
            "column_NAME":"LSTG_SITE_ID",
            "data_TYPE":4,
            "nullable":1,
            "column_SIZE":-1,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":-1,
            "ordinal_POSITION":3,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"INTEGER"
         },
         {  
            "table_NAME":"TEST_KYLIN_FACT",
            "table_SCHEM":"EDW",
            "column_NAME":"SLR_SEGMENT_CD",
            "data_TYPE":5,
            "nullable":1,
            "column_SIZE":-1,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":-1,
            "ordinal_POSITION":4,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"SMALLINT"
         },
         {  
            "table_NAME":"TEST_KYLIN_FACT",
            "table_SCHEM":"EDW",
            "column_NAME":"LSTG_FORMAT_NAME",
            "data_TYPE":12,
            "nullable":1,
            "column_SIZE":256,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":256,
            "ordinal_POSITION":5,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"VARCHAR(256) CHARACTER SET \"UTF-16LE\" COLLATE \"UTF-16LE$en_US$primary\""
         },
         {  
            "table_NAME":"TEST_KYLIN_FACT",
            "table_SCHEM":"EDW",
            "column_NAME":"SELLER_ID",
            "data_TYPE":-5,
            "nullable":1,
            "column_SIZE":-1,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":-1,
            "ordinal_POSITION":6,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"BIGINT"
         },
         {  
            "table_NAME":"TEST_KYLIN_FACT",
            "table_SCHEM":"EDW",
            "column_NAME":"PRICE",
            "data_TYPE":3,
            "nullable":1,
            "column_SIZE":19,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":4,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":19,
            "ordinal_POSITION":7,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"DECIMAL(19, 4)"
         }
      ],
      "table_NAME":"TEST_KYLIN_FACT",
      "table_SCHEM":"EDW",
      "ref_GENERATION":null,
      "self_REFERENCING_COL_NAME":null,
      "type_SCHEM":null,
      "table_TYPE":"TABLE",
      "table_CAT":"defaultCatalog",
      "remarks":null,
      "type_CAT":null,
      "type_NAME":null
   },
   {  
      "columns":[  
         {  
            "table_NAME":"TEST_SELLER_TYPE_DIM",
            "table_SCHEM":"EDW",
            "column_NAME":"SELLER_TYPE_CD",
            "data_TYPE":5,
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
            "type_NAME":"SMALLINT"
         },
         {  
            "table_NAME":"TEST_SELLER_TYPE_DIM",
            "table_SCHEM":"EDW",
            "column_NAME":"SELLER_TYPE_DESC",
            "data_TYPE":12,
            "nullable":1,
            "column_SIZE":256,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":256,
            "ordinal_POSITION":2,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"VARCHAR(256) CHARACTER SET \"UTF-16LE\" COLLATE \"UTF-16LE$en_US$primary\""
         }
      ],
      "table_NAME":"TEST_SELLER_TYPE_DIM",
      "table_SCHEM":"EDW",
      "ref_GENERATION":null,
      "self_REFERENCING_COL_NAME":null,
      "type_SCHEM":null,
      "table_TYPE":"TABLE",
      "table_CAT":"defaultCatalog",
      "remarks":null,
      "type_CAT":null,
      "type_NAME":null
   },
   {  
      "columns":[  
         {  
            "table_NAME":"TEST_SITES",
            "table_SCHEM":"EDW",
            "column_NAME":"SITE_ID",
            "data_TYPE":4,
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
            "type_NAME":"INTEGER"
         },
         {  
            "table_NAME":"TEST_SITES",
            "table_SCHEM":"EDW",
            "column_NAME":"SITE_NAME",
            "data_TYPE":12,
            "nullable":1,
            "column_SIZE":256,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":256,
            "ordinal_POSITION":2,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"VARCHAR(256) CHARACTER SET \"UTF-16LE\" COLLATE \"UTF-16LE$en_US$primary\""
         },
         {  
            "table_NAME":"TEST_SITES",
            "table_SCHEM":"EDW",
            "column_NAME":"CRE_USER",
            "data_TYPE":12,
            "nullable":1,
            "column_SIZE":256,
            "buffer_LENGTH":-1,
            "decimal_DIGITS":0,
            "num_PREC_RADIX":10,
            "column_DEF":null,
            "sql_DATA_TYPE":-1,
            "sql_DATETIME_SUB":-1,
            "char_OCTET_LENGTH":256,
            "ordinal_POSITION":3,
            "is_NULLABLE":"YES",
            "scope_CATLOG":null,
            "scope_SCHEMA":null,
            "scope_TABLE":null,
            "source_DATA_TYPE":-1,
            "iS_AUTOINCREMENT":null,
            "table_CAT":"defaultCatalog",
            "remarks":null,
            "type_NAME":"VARCHAR(256) CHARACTER SET \"UTF-16LE\" COLLATE \"UTF-16LE$en_US$primary\""
         }
      ],
      "table_NAME":"TEST_SITES",
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

### List cubes
`GET /cubes`
#### Request Parameters
* cubeName - `optional` `string` Cube name to find.
* offset - `required` `int` Offset used by pagination
* limit - `required` `int ` Cubes per page.

#### Response Sample
```sh
Status: 200OK
```
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

### Rebuild cube
`PUT /cubes/{cubeName}/rebuild`
#### Path Variable
* cubeName - `required` `string` Cube name.

#### Request Body
* startTime - `required` `long` Start timestamp of data to build, e.g. 1388563200000 for 2014-1-1
* endTime - `required` `long` End timestamp of data to build
* buildType - `required` `string` Build type: 'BUILD' OR 'MERGE'

#### Response Sample
```sh
Status: 200OK
```
```sh
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
         "name":"Build Dimension Dictionary",
         "sequence_id":2,
         "exec_cmd":" -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/fact_distinct_columns",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NO_MR_DICTIONARY",
         "info":null,
         "run_async":false
      },
      {  
         "interruptCmd":null,
         "name":"Build Base Cuboid Data",
         "sequence_id":3,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/kylin_intermediate_test_kylin_cube_with_slr_desc_19700101000000_20140731160000_c143e0e4_ac5f_434d_acf3_46b0d15e3dc6 -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/base_cuboid -jobname Kylin_Base_Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_3 -level 0",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_BASECUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 8-Dimension",
         "sequence_id":4,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/base_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/8d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_4 -level 1",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 7-Dimension",
         "sequence_id":5,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/8d_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/7d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_5 -level 2",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 6-Dimension",
         "sequence_id":6,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/7d_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/6d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_6 -level 3",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 5-Dimension",
         "sequence_id":7,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/6d_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/5d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_7 -level 4",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 4-Dimension",
         "sequence_id":8,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/5d_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/4d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_8 -level 5",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Calculate HTable Region Splits",
         "sequence_id":9,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/* -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/rowkey_stats -jobname Kylin_Region_Splits_Calculator_test_kylin_cube_with_slr_empty_Step_9 -cubename test_kylin_cube_with_slr_empty",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_RANGEKEYDISTRIBUTION",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Create HTable",
         "sequence_id":10,
         "exec_cmd":" -cubename test_kylin_cube_with_slr_empty -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/rowkey_stats/part-r-00000 -htablename KYLIN-CUBE-TEST_KYLIN_CUBE_WITH_SLR_EMPTY-19700101000000_20140731160000_11BB4326-5975-4358-804C-70D53642E03A",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADDOP_NO_MR_CREATEHTABLE",
         "info":null,
         "run_async":false
      },
      {  
         "interruptCmd":null,
         "name":"Convert Cuboid Data to HFile",
         "sequence_id":11,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/* -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/hfile -htablename KYLIN-CUBE-TEST_KYLIN_CUBE_WITH_SLR_EMPTY-19700101000000_20140731160000_11BB4326-5975-4358-804C-70D53642E03A -jobname Kylin_HFile_Generator_test_kylin_cube_with_slr_empty_Step_11",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_CONVERTHFILE",
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


### Disable Cube
`PUT /cubes/{cubeName}/disable`
#### Path variable
* cubeName - `required` `string` Cube name.

#### Response Sample
(Same as "Enable Cube")


### Purge Cube
`PUT /cubes/{cubeName}/purge`
#### Path variable
* cubeName - `required` `string` Cube name.

#### Response Sample
(Same as "Enable Cube")


### Enable Cube
`PUT /cubes/{cubeName}/enable`
#### Path variable
* cubeName - `required` `string` Cube name.

#### Response Sample
```sh
Status: 200OK
```
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

### Resume Job
`PUT /jobs/{jobId}/resume`

#### Path variable
* jobId- `required` `string` Job id.

#### Response Sample
```sh
Status: 200OK
```
```sh
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
         "name":"Build Dimension Dictionary",
         "sequence_id":2,
         "exec_cmd":" -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/fact_distinct_columns",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NO_MR_DICTIONARY",
         "info":null,
         "run_async":false
      },
      {  
         "interruptCmd":null,
         "name":"Build Base Cuboid Data",
         "sequence_id":3,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/kylin_intermediate_test_kylin_cube_with_slr_desc_19700101000000_20140731160000_c143e0e4_ac5f_434d_acf3_46b0d15e3dc6 -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/base_cuboid -jobname Kylin_Base_Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_3 -level 0",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_BASECUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 8-Dimension",
         "sequence_id":4,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/base_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/8d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_4 -level 1",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 7-Dimension",
         "sequence_id":5,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/8d_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/7d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_5 -level 2",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 6-Dimension",
         "sequence_id":6,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/7d_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/6d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_6 -level 3",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 5-Dimension",
         "sequence_id":7,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/6d_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/5d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_7 -level 4",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 4-Dimension",
         "sequence_id":8,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/5d_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/4d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_8 -level 5",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Calculate HTable Region Splits",
         "sequence_id":9,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/* -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/rowkey_stats -jobname Kylin_Region_Splits_Calculator_test_kylin_cube_with_slr_empty_Step_9 -cubename test_kylin_cube_with_slr_empty",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_RANGEKEYDISTRIBUTION",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Create HTable",
         "sequence_id":10,
         "exec_cmd":" -cubename test_kylin_cube_with_slr_empty -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/rowkey_stats/part-r-00000 -htablename KYLIN-CUBE-TEST_KYLIN_CUBE_WITH_SLR_EMPTY-19700101000000_20140731160000_11BB4326-5975-4358-804C-70D53642E03A",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADDOP_NO_MR_CREATEHTABLE",
         "info":null,
         "run_async":false
      },
      {  
         "interruptCmd":null,
         "name":"Convert Cuboid Data to HFile",
         "sequence_id":11,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/* -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/hfile -htablename KYLIN-CUBE-TEST_KYLIN_CUBE_WITH_SLR_EMPTY-19700101000000_20140731160000_11BB4326-5975-4358-804C-70D53642E03A -jobname Kylin_HFile_Generator_test_kylin_cube_with_slr_empty_Step_11",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_CONVERTHFILE",
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

### Discard Job
`PUT /jobs/{jobId}/cancel`
#### Path variable
* jobId- `required` `string` Job id.

#### Response Sample
```sh
Status: 200OK
```
```sh
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
         "name":"Build Dimension Dictionary",
         "sequence_id":2,
         "exec_cmd":" -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/fact_distinct_columns",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NO_MR_DICTIONARY",
         "info":null,
         "run_async":false
      },
      {  
         "interruptCmd":null,
         "name":"Build Base Cuboid Data",
         "sequence_id":3,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/kylin_intermediate_test_kylin_cube_with_slr_desc_19700101000000_20140731160000_c143e0e4_ac5f_434d_acf3_46b0d15e3dc6 -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/base_cuboid -jobname Kylin_Base_Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_3 -level 0",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_BASECUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 8-Dimension",
         "sequence_id":4,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/base_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/8d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_4 -level 1",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 7-Dimension",
         "sequence_id":5,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/8d_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/7d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_5 -level 2",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 6-Dimension",
         "sequence_id":6,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/7d_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/6d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_6 -level 3",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 5-Dimension",
         "sequence_id":7,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/6d_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/5d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_7 -level 4",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Build N-Dimension Cuboid Data : 4-Dimension",
         "sequence_id":8,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -segmentname 19700101000000_20140731160000 -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/5d_cuboid -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/4d_cuboid -jobname Kylin_ND-Cuboid_Builder_test_kylin_cube_with_slr_empty_Step_8 -level 5",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_NDCUBOID",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Calculate HTable Region Splits",
         "sequence_id":9,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/* -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/rowkey_stats -jobname Kylin_Region_Splits_Calculator_test_kylin_cube_with_slr_empty_Step_9 -cubename test_kylin_cube_with_slr_empty",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_RANGEKEYDISTRIBUTION",
         "info":null,
         "run_async":true
      },
      {  
         "interruptCmd":null,
         "name":"Create HTable",
         "sequence_id":10,
         "exec_cmd":" -cubename test_kylin_cube_with_slr_empty -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/rowkey_stats/part-r-00000 -htablename KYLIN-CUBE-TEST_KYLIN_CUBE_WITH_SLR_EMPTY-19700101000000_20140731160000_11BB4326-5975-4358-804C-70D53642E03A",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADDOP_NO_MR_CREATEHTABLE",
         "info":null,
         "run_async":false
      },
      {  
         "interruptCmd":null,
         "name":"Convert Cuboid Data to HFile",
         "sequence_id":11,
         "exec_cmd":" -conf C:/kylin/Kylin/server/src/main/resources/hadoop_job_conf_medium.xml -cubename test_kylin_cube_with_slr_empty -input /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/cuboid/* -output /tmp/kylin-c143e0e4-ac5f-434d-acf3-46b0d15e3dc6/test_kylin_cube_with_slr_empty/hfile -htablename KYLIN-CUBE-TEST_KYLIN_CUBE_WITH_SLR_EMPTY-19700101000000_20140731160000_11BB4326-5975-4358-804C-70D53642E03A -jobname Kylin_HFile_Generator_test_kylin_cube_with_slr_empty_Step_11",
         "interrupt_cmd":null,
         "exec_start_time":0,
         "exec_end_time":0,
         "exec_wait_time":0,
         "step_status":"PENDING",
         "cmd_type":"JAVA_CMD_HADOOP_CONVERTHFILE",
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

### Get job step output
`GET /{jobId}/steps/{stepId}/output`
#### Path Variable
* jobId - `required` `string` Job id.
* stepId - `required` `string` Step id; the step id is composed by jobId with step sequence id; for example, the jobId is "fb479e54-837f-49a2-b457-651fc50be110", its 3rd step id is "fb479e54-837f-49a2-b457-651fc50be110-3", 

#### Response Sample
```sh
Status: 200OK
```
```sh
{  
   "cmd_output":"log string"
}
```


### Get Hive Table
`GET /tables/{tableName}`
#### Request Parameters
* tableName - `required` `string` table name to find.

#### Response Sample
```sh
Status: 200OK
```
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

### Get Hive Table  (Extend Info)
`GET /tables/{tableName}/exd-map`
#### Request Parameters
* tableName - `optional` `string` table name to find.

#### Response Sample
```sh
Status: 200OK
```
```sh
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

### Get Hive Tables
`GET /tables`
#### Request Parameters
* project- `required` `string` will list all tables in the project.
* ext- `optional` `boolean`  set true to get extend info of table.

#### Response Sample
```sh
Status: 200OK
```
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
}, {
    uuid: "82d72d9c-705c-4706-aaf5-358ac2661cf9",
    name: "TEST_KYLIN_FACT",
    columns: [{
        id: "1",
        name: "TRANS_ID",
        datatype: "bigint"
    }, {
        id: "2",
        name: "CAL_DT",
        datatype: "date"
    }, {
        id: "3",
        name: "LSTG_FORMAT_NAME",
        datatype: "string"
    }, {
        id: "4",
        name: "LEAF_CATEG_ID",
        datatype: "int"
    }, {
        id: "5",
        name: "LSTG_SITE_ID",
        datatype: "int"
    }, {
        id: "6",
        name: "SLR_SEGMENT_CD",
        datatype: "smallint"
    }, {
        id: "7",
        name: "PRICE",
        datatype: "decimal(38,16)"
    }, {
        id: "8",
        name: "ITEM_COUNT",
        datatype: "bigint"
    }, {
        id: "9",
        name: "SELLER_ID",
        datatype: "bigint"
    }],
    database: "DEFAULT",
    cardinality: {},
    last_modified: 0,
    exd: {
        minFileSize: "607328",
        totalNumberFiles: "1",
        location: "hdfs://sandbox.hortonworks.com:8020/apps/hive/warehouse/test_kylin_fact",
        lastAccessTime: "1418899958520",
        lastUpdateTime: "1414051037179",
        columns: "struct columns { i64 trans_id, date cal_dt, string lstg_format_name, i32 leaf_categ_id, i32 lstg_site_id, i16 slr_segment_cd, decimal(38,16) price, i64 item_count, i64 seller_id}",
        partitionColumns: "",
        EXD_STATUS: "true",
        maxFileSize: "607328",
        inputformat: "org.apache.hadoop.mapred.TextInputFormat",
        partitioned: "false",
        tableName: "test_kylin_fact",
        owner: "root",
        totalFileSize: "607328",
        outputformat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    }
 }
]
```

### Load Hive Tables
`POST /tables/{tables}/{project}`

* tables- `required` `string` tables you want to load from hive,split with comma.
* project- `required` `String`  specif.


#### Response Sample
```sh
Status: 200OK
```
```sh
{
    "result.loaded": ["DEFAULT.SAMPLE_07"],
    "result.unloaded": ["sapmle_08"]
}
```


### Wipe cache
`GET /cache/{type}/{name}/{action}`
#### Path variable
* type - `required` `string` 'METADATA' or 'CUBE'
* name - `required` `string` Cache key.
* action - `required` `string` 'create', 'update' or 'drop'

#### Response Sample
```sh
Status: 200OK
```