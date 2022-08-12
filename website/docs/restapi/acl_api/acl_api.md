---
title: Data ACL API
language: en
sidebar_label: Data ACL API
pagination_label: Data ACL API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - data acl api
draft: false
last_update:
    date: 12/08/2022
---

> Reminders:
>
> 1. Please read [Access and Authentication REST API](authentication.md) and understand how authentication works.
> 2. On Curl command line, don't forget to quote the URL if it contains the special char `&`.


### Get ACL

- `GET http://localhost:port/kylin/api/acl/{type}/{name}?authorized_only=true&project=m`

- URL Parameters
  
  - `type` - `required` `string`, user type, optional values: user, group (case-insensitive)
  - `name` - `required` `string`, user name or user group name.
  - `project` - `required` `string`, project name.
  - `authorized_only` - `optional` `boolean`, whether to return only authorized table rows and columns，default value is  `false`。

- HTTP Header
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://localhost:7070/kylin/api/acl/User/bb?authorized_only=true&project=m' \
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
          {
              "tables": [
                  {
                      "authorized": true,
                      "columns": [
                          {
                              "authorized": true,
                              "column_name": "C_ADDRESS",
                              "data_mask_type": "AS_NULL",
                              "dependent_columns": null,
                              "datatype": "varchar(4096)"
                          },
                          {
                              "authorized": true,
                              "column_name": "C_CITY",
                              "data_mask_type": "DEFAULT",
                              "dependent_columns": null,
                              "datatype": "varchar(4096)"
                          },
                          {
                              "authorized": false,
                              "column_name": "C_CUSTKEY",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "integer"
                          },
                          {
                              "authorized": true,
                              "column_name": "C_MKTSEGMENT",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "varchar(4096)"
                          },
                          {
                              "authorized": true,
                              "column_name": "C_NAME",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "varchar(4096)"
                          },
                          {
                              "authorized": true,
                              "column_name": "C_NATION",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "varchar(4096)"
                          },
                          {
                              "authorized": true,
                              "column_name": "C_PHONE",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "varchar(4096)"
                          },
                          {
                              "authorized": true,
                              "column_name": "C_REGION",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "varchar(4096)"
                          }
                      ],
                      "row_filter": {
                          "type": "AND",
                          "filter_groups": []
                      },
                      "table_name":"ANALYSIS_PACKAGE_TABLE",
                      "authorized_column_num":10,
                      "total_column_num":10
                  },
                  {
                      "authorized": true,
                      "columns": [
                          {
                              "authorized": true,
                              "column_name": "LO_COMMITDATE",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "date"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_CUSTKEY",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "integer"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_DISCOUNT",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "bigint"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_EXTENDEDPRICE",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "bigint"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_LINENUMBER",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "bigint"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_ORDERDATE",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "date"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_ORDERKEY",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "bigint"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_ORDERPRIOTITY",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "varchar(4096)"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_ORDTOTALPRICE",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "bigint"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_PARTKEY",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "integer"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_QUANTITY",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "bigint"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_REVENUE",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "bigint"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_SHIPMODE",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "varchar(4096)"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_SHIPPRIOTITY",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "integer"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_SUPPKEY",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "integer"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_SUPPLYCOST",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "bigint"
                          },
                          {
                              "authorized": true,
                              "column_name": "LO_TAX",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "bigint"
                          },
                          {
                              "authorized": true,
                              "column_name": "V_REVENUE",
                              "data_mask_type": null,
                              "dependent_columns": null,
                              "datatype": "bigint"
                          }
                      ],
                      "row_filter": {
                          "type": "AND",
                          "filter_groups": []
                      },
                      "table_name":"QUERY_FACT_TABLE",
                      "authorized_column_num":11,
                      "total_column_num":11
                  }
              ],
              "authorized_table_num": 2,
              "total_table_num": 2,
              "database_name": "SSB"
          }
      ],
      "msg": ""
  }
  ```


### Update Acl

- `PUT http://localhost:port/kylin/api/acl/{type}/{name}?project=m`

- URL Parameters
  
  - `type` - `required` `string`, User type, optional values: user, group. case insensitive.
  - `name` - `required` `string`, User name or user group name.
  - `project` - `required` `string`, Project name.

- HTTP Body

  **Note:** You can grant or revoke tables, columns or rows's acls incrementally.

  - `database_name` - `required` `string`, database name. case insensitive.
  - `tables` - `required` `array[string]`, table information.
    
      - `table_name` - `required` `string`, name of the table. case insensitive.
      - `authorized` - `optional` `boolean`, whether to authorize the permissions of this table. `true` represents authorization and the default value is `false`.
      - `columns` - `optional` `array[string]`, column level permissions to grant or revoke, if no change, set this value to null.
          
          - `column_name` - `required` `string`, the name of the column. case insensitive.
          - `authorized` - `optional` `boolean`, whether to authorize the permissions in this column. `true` indicates authorization and the default value is `false`.
          - `data_mask_type`- `optional` `string`, sensitive data column masking type, optional values: DEFAULT, AS_NULL. Example
          ```json
          {
              "authorized":true,
              "columns":[
                  {
                      "authorized":true,
                      "column_name":"STORE_AND_FWD_FLAG",
                      "data_mask_type":null,
                      "dependent_columns":null
                  },
                  {
                      "authorized":true,
                      "column_name":"TOTAL_AMOUNT",
                      "data_mask_type":"DEFAULT",
                      "dependent_columns":null
                  },
                  {
                      "authorized":true,
                      "column_name":"TRIP_DISTANCE",
                      "data_mask_type":"AS_NULL",
                      "dependent_columns":null
                  }
              ]
          }
          ```
        
          DEFAULT: use column datatype's default value as mask value. INT's default mask value is 0, VARCHAR's default mask value is ****.
          AS_NULL: use null as mask value
        
        - `dependent_columns` - `optional` `array`. Column-level permission control of associated row values parameter. Example
        
          ```json
          {
              "authorized":true,
              "column_name":"PASSENGER_COUNT",
              "data_mask_type":null,
              "dependent_columns":[
                  {
                      "column_identity":"DEFAULT.GREEN_TRIP_DATA.DO_LOCATION_ID",
                      "values":[
                          "1",
                          "2"
                      ]
                  }
              ]
          }
          ```
        
          column PASSENGER_COUNT will depend on DEFAULT.GREEN_TRIP_DATA.DO_LOCATION_ID's values.
      - `row_filter` - `optional` set row level access control. Set to null if you do not want to modify it.
      
          - `type` - `optional` `string` set logical operator type to `AND` or `OR` between filters/filter groups. Default value is `AND`.
          - `filter_groups` - `optional` `array[string]` set filters or filter groups. Default value is an empty list.
          
              - `type` - `optional` `string` set logical operator type to `AND` or `OR` between filters within one filter group. Default value is `AND`.
              - `is_group` - `required` `boolean` set type, indicating whether it is a filter or filter group.
              - `filters` - `optional` `array[string]` set filters. Default value is an empty list.
              
                  - `column_name` - `required` `string` set the name of the column where the row level access control applies on.
                  - `in_items` - `optional` `array[string]` the values of filter condition. Default is an empty list.
                  - `like_items` - `optional` `array[string]` the patterns of filter condition. Default is an empty list.
                  
                  For example, the request below sets one filter group with two filters, and another standalone filter.
                  The logical operator between the filter group and the standalone filter is `OR`. The logical operator between filters in the filter group is `AND`.
                  ```json
                  {
                      "row_filter": {
                          "type": "OR",
                          "filter_groups": [{
                              "type": "AND",
                              "filters": [
                                  {
                                      "column_name": "LSTG_FORMAT_NAME",
                                      "in_items": ["ABIN", "Others"],
                                      "like_items": ["B%"]
                                  },
                                  {
                                      "column_name": "TRANS_ID",
                                      "in_items": ["0", "1"],
                                      "like_items": []
                                  }
                              ],
                              "is_group": true
                          }, {
                              "type": "AND",
                              "filters": [
                                  {
                                      "column_name": "TRANS_ID",
                                      "in_items": ["0"],
                                      "like_items": []
                                  }
                              ],
                              "is_group": false
                          }]
                      }
                  }
                  ```
                  After authorized successfully, you can only see the results of
                  ```mysql-sql
                  (
                      (LSTG_FORMAT_NAME in ('ABIN', 'Others') OR LSTG_FORMAT_NAME like 'B%')
                      AND
                      (TRANS_ID in (0, 1))
                  ) OR (
                      (TRANS_ID in (0))
                  )
                  ```
                  If you need to set row level access control, all three `column_name`，`in_items` and `like_items` need to be filled in. This field uses a full update.
                  Thus in order to disable the row level access control, you need to send an empty `filter_groups` as shown below:
                  ```json
                  {
                      "row_filter":
                          {
                              "type": "AND",
                              "filter_groups": []
                          }
                  }
                  ```

      **Note:** If you are still using the old row level access control API before Kylin version 4.3.3, you may encounter an update failure. It is recommended to use the latest API to manage row level access control.


- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`
  
- Curl Request Example

    - Grant / Revoke access on the specific table. If revoke table access , all columns' / rows' access in the specific table will be revoked.

        ```sh
        curl --location --request PUT 'http://localhost:8080/kylin/api/acl/User/user_1?project=project_1' \
        -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
        -H 'Accept-Language: cn' \
        -H 'Authorization: Basic QURNSU46a3lsaW5AMjAyMA==' \
        -H 'Content-Type: application/json;charset=utf-8' \
        --data-raw '[
            {
                "tables": [
                    {
                        "authorized": false,
                        "table_name": "CUSTOMER"
                    }
                ],
                "database_name": "SSB"
            }
        ]'
        ```

    - Grant / Revoke access on the specific columns. Only update the specific column in the specific table, not update others.

        ```sh
        curl --location --request PUT 'http://localhost:8080/kylin/api/acl/User/user_1?project=project_1' \
        -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
        -H 'Accept-Language: cn' \
        -H 'Authorization: Basic QURNSU46a3lsaW5AMjAyMA==' \
        -H 'Content-Type: application/json;charset=utf-8' \
        --data-raw '[
            {
                "tables": [
                    {
                        "columns": [
                            {
                                "authorized": false,
                                "column_name": "C_CITY"
                            }
                        ],
                        "authorized": true,
                        "table_name": "CUSTOMER"
                    },
                    {
                        "columns": [
                            {
                                "authorized": true,
                                "column_name": "LO_REVENUE",
                                "data_mask_type": "AS_NULL"
                            },
                            {
                                "authorized": true,
                                "column_name": "LO_TAX",
                                "data_mask_type": "DEFAULT"
                            },
                            {
                                "authorized": true,
                                "column_name": "LO_QUANTITY",
                                "data_mask_type": null,
                                "dependent_columns": [
                                    {
                                        "column_identity": "SSB.CUSTOMER.C_CUSTKEY",
                                        "values": [
                                            "1",
                                            "2"
                                        ]
                                    }
                                ]
                            }
                        ],
                        "authorized": true,
                        "table_name": "P_LINEORDER"
                    }
                ],
                "database_name": "SSB"
            }
        ]'
        ```

    - Grant / Revoke access on the specific rows. Fully update the row access of the specified table, not update others.

    ```sh
    curl --location --request PUT 'http://localhost:8080/kylin/api/acl/User/user_1?project=project_1' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: cn' \
    -H 'Authorization: Basic QURNSU46a3lsaW5AMjAyMA==' \
    -H 'Content-Type: application/json;charset=utf-8' \
    --data-raw '[
        {
            "tables": [
                {
                    "authorized": true,
                    "row_filter":
                    {
                        "type": "AND",
                        "filter_groups": [
                            {
                                "type": "AND",
                                "filters": [
                                    {
                                        "column_name": "C_NATION",
                                        "in_items": ["CHINA", "UNITED KINGDOM"],
                                        "like_items": []
                                    }
                                ],
                                "is_group": false
                            },
                            {
                                "type": "AND",
                                "filters": [
                                    {
                                        "column_name": "C_CUSTKEY",
                                        "in_items": ["15", "16", "19"],
                                        "like_items": []
                                    }
                                ],
                                "is_group": false
                            }
                        ]
                    },
                    "table_name": "CUSTOMER"
                },
                {
                    "row_filter":
                    {
                        "type": "AND",
                        "filter_groups": [
                            {
                                "type": "AND",
                                "filters": [
                                    {
                                        "column_name": "LO_CUSTKEY",
                                        "in_items": ["15", "16", "20"],
                                        "like_items": []
                                    }
                                ],
                                "is_group": false
                            },
                            {
                                "type": "AND",
                                "filters": [
                                    {
                                        "column_name": "LO_QUANTITY",
                                        "in_items": ["31", "33", "23"],
                                        "like_items": []
                                    }
                                ],
                                "is_group": false
                            },
                            {
                                "type": "AND",
                                "filters": [
                                    {
                                        "column_name": "LO_ORDERDATE",
                                        "in_items": ["1995-02-01", "1996-01-26", "1992-11-21"],
                                        "like_items": []
                                    }
                                ],
                                "is_group": false
                            }
                        ]
                    },
                    "authorized": true,
                    "table_name": "P_LINEORDER"
                }
            ],
            "database_name": "SSB"
        }
    ]'
    ```


- Response Example

    ```json
    {
        "code": "000",
        "data": "",
        "msg": ""
    }
    ```
