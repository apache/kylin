---
title: Model Import And Export API
language: en
sidebar_label: Model Import And Export API
pagination_label: Model Import And Export API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - model import and export api
draft: false
last_update:
    date: 08/12/2022
---


> Reminders:
>
> 1. Please read [Access and Authentication REST API](../authentication.md) and understand how authentication works.
> 2. On Curl command line, don't forget to quote the URL if it contains the special char `&`.

  

### Get Batch Exportable Models {#Get-Batch-Exportable-Models}

- `GET http://host:port/kylin/api/metastore/previews/models?project=test&model_names=model1,model2`

- URL Parameters

  - `project` - `required` `string`, project name. 
  - `model_names` - `optional` `array[string]`, model's name, separated by commas.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl --location --request GET 'http://host:port/kylin/api/metastore/previews/models?project=target_project&model_names=model_index' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8'
  ```
  
- Response Example

  - Field description
    - `code` - `string`, return code, `000` Means successful import, `999` Means import failed.
    - `data` - `json`, return data
      - `uuid` - `string` uuid;
      - `name` - `string` model's name;
      - `status` - `string` model's status;
      - `has_recommendations` - `boolean` whether model has recommendations;
      - `has_override_props` - `boolean` whether model has override properties;
      - `has_multiple_partition_values` - `boolean` whether model has multiple columns;

  ```json
  {
      "code": "000",
      "data": [
          {
              "uuid": "10d5eb7c-d854-4f72-9e4b-9b1f3c65bcda",
              "name": "model_index",
              "status": "ONLINE",
              "has_recommendations": true,
              "has_override_props": true,
              "has_multiple_partition_values": true,
              "tables": [
                  {
                      "name": "SSB.P_LINEORDER",
                      "kind": "FACT"
                  },
                  {
                      "name": "SSB.CUSTOMER",
                      "kind": "LOOKUP"
                  }
              ]
          }
      ],
      "msg": ""
  }
  ```



### Model Metadata Export {#Model-Metadata-Export}

- `POST http://host:port/kylin/api/metastore/backup/models?project=test`

- URL Parameters

  - `project` - `required` `string`, project name. 

- HTTP Body: JSON Object

  - `names`  - `required` `array[string]`, model name list. 
  - `export_recommendations`  - `optional`  `boolean`, true/false, default value is false,whether export model's recommendations.
  - `export_over_props`  - `optional` `boolean`, true/false, default value is false, whether export model's override props.
  - `export_multiple_partition_values`  - `optional` `boolean`, true/false, default is false, whether export model's multiple partition values.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl --remote-name --remote-header-name --location --request POST 'http://host:port/kylin/api/metastore/backup/models?project=original_project' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Content-Type: application/json;charset=utf-8' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  --data '{
      "names": [
          "ssb_model"
      ],
      "export_recommendations": true,
      "export_over_props": true,
      "export_multiple_partition_values": true
  }'
  ```

- Response Example

  return zip file on success

> Note: The remote-header-name parameter is available in curl 7.20.0 and above

### Model Metadata Import Validation{#Model-Metadata-Import-Validation}

- `POST http://host:port/kylin/api/metastore/validation/models?project=test`

- URL Parameters

  - `project` - `required` `string`, project name. 

- HTTP Body: form-data

  - `file` - `required` `MultipartFile`, model metadata file. 

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`

- Curl Request Example

  ```sh
  curl --location --request POST 'http://host:port/kylin/api/metastore/validation/models?project=original_project' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  --form 'file=@metadata.zip'
  ```

- Response Example

  ```json
  {
      "code": "000",
      "data": {
          "models": {
              "ssb_model": {
                  "differences": 3,
                  "missing_items": [],
                  "new_items": [],
                  "update_items": [],
                  "reduce_items": [
                      {
                          "reason": null,
                          "attributes": {
                              "name": "LO_SUPPLYCOST",
                              "alias_dot_column": "P_LINEORDER.LO_SUPPLYCOST"
                          },
                          "detail": "LO_SUPPLYCOST",
                          "type": "MODEL_DIMENSION",
                          "model_alias": "ssb_model",
                          "importable": true,
                          "creatable": true,
                          "overwritable": true
                      },
                      {
                          "reason": null,
                          "attributes": {
                              "col_orders": [
                                  "P_LINEORDER.LO_CUSTKEY",
                                  "P_LINEORDER.LO_DISCOUNT",
                                  "P_LINEORDER.LO_LINENUMBER",
                                  "P_LINEORDER.LO_ORDERDATE",
                                  "P_LINEORDER.LO_ORDERKEY",
                                  "P_LINEORDER.LO_PARTKEY",
                                  "P_LINEORDER.LO_QUANTITY",
                                  "P_LINEORDER.LO_SUPPKEY",
                                  "P_LINEORDER.LO_SUPPLYCOST"
                              ]
                          },
                          "detail": "20000050001",
                          "type": "WHITE_LIST_INDEX",
                          "model_alias": "ssb_model",
                          "importable": true,
                          "creatable": true,
                          "overwritable": true
                      },
                      {
                          "reason": null,
                          "attributes": {
                              "col_orders": [
                                  "P_LINEORDER.LO_CUSTKEY",
                                  "P_LINEORDER.LO_QUANTITY",
                                  "P_LINEORDER.LO_SUPPKEY",
                                  "LO_SUPPKEY_SUM",
                                  "COUNT_ALL"
                              ]
                          },
                          "detail": "180001",
                          "type": "WHITE_LIST_INDEX",
                          "model_alias": "ssb_model",
                          "importable": true,
                          "creatable": true,
                          "overwritable": true
                      }
                  ],
                  "importable": true,
                  "overwritable": true,
                  "creatable": true
              }
          }
      },
      "msg": ""
  }
  ```

  - Field description

    - `code` - `string`, return code, `000` Means successful import, `999` Means import failed.
    - `data` - `json`, return data
      - `models` - `array[object]`,  validate information of each import model
        - `key` - `string`,  model's name
          - `differences` - `int`, the total differences of model metadata between zip file and target model
          - `missing_items` - `array[object]`, table or table column is required but target project didn't have
          - `new_items` - `array[object]`, the new items between import and target model
          - `reduce_items` - `array[object]`, the reduced items between import and target model
          - `update_items` - `array[object]`, the updated items between import and target model
          - `has_same_name` - `boolean`, whether has same name model
          - `importable` - `boolean`, whether the model can import
          - `overwritable` - `boolean`, whether the model can overwrite target model
          - `creatable` - `boolean`, whether the model can create a new model

### Model Metadata Import {#Model-Metadata-Import}

- `POST http://host:port/kylin/api/metastore/import/models?project=test`

- URL Parameters

  - `project` - `required` `string`, project name. 

- HTTP Body: form-data

  - `file` - `required` `MultipartFile`, model metadata file. 

  - `request`  - `required` `MultipartFile`, json file. file content is:

    ```
    {
        "models":[
            {
                "original_name":"ssb_model",
                "target_name":"ssb_model",
                "import_type":"OVERWRITE"
            }
        ]
    }
    ```

    - `original_name` -`string` original model's name
    - `target_name` -`string` target model's name, set value while import_type is NEW
    - `import_type` - `string` optional values are `NEW`, `OVERWRITE`, `UN_IMPORT`. `NEW` means create new model, `OVERWRITE` means overwrite existing model, `UN_IMPORT` means not import model. 

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`

- Curl Request Example

  ```sh
  curl --location --request POST 'http://host:port/kylin/api/metastore/import/models?project=original_project' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  --form 'file=@metadata.zip;type=application/octet-stream' \
  --form 'request=@request.json;type=application/json'
  ```

- Response Example

  ```json
  {
        "code":"000",
        "data":"",
        "msg":""
  }
  ```
