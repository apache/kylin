---
title: Project ACL API
language: en
sidebar_label: Project ACL API
pagination_label: Project ACL API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - project acl api
draft: false
last_update:
    date: 08/12/2022
---

> Reminders:
>
> 1. Please read [Access and Authentication REST API](../authentication.md) and understand how authentication works.
> 2. On Curl command line, don't forget to quote the URL if it contains the special char `&`.


###  Get Project ACL {#get-project-acl}

- `GET http://host:port/kylin/api/access/project`

- Introduced in: 5.0

- Request Parameters

  - `project` - `required` `string`, project name.
  - `name` - `optional` `string`, user or group name.
  - `page_offset` - `optional` `int`, offset of returned result, 0 by default.
  - `page_size` - `optional` `int`, quantity of returned result per page, 10 by default.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/access/project?project=learn_kylin' \
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
                  "type": "user",
                  "name": "ADMIN",
                  "permission": "ADMIN"
              },
              {
                  "type": "group",
                  "name": "TEST_GROUP",
                  "permission": "QUERY"
              }
            	...
          ],
          "offset": 0,
          "limit": 10,
          "total_size": 4
      },
      "msg": ""
  }
  ```



###  Grant Project ACL {#grant-project-acl}

- `POST http://host:port/kylin/api/access/project`

- Introduced in: 5.0

- HTTP Body: JSON Object

  - `project` - `required` `string`, project name.
  - `type` - `required` `string`,  represents users or user groups, optional values are `user` or `group`.
  - `permission` - `required` `string`,  represents users or user groups permission, optional values are `QUERY`, `OPERATION`,  `MANAGEMENT` and  `ADMIN`.
  - `names` - `required` `array[string]`, name of user or user group.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X POST \
    'http://host:port/kylin/api/access/project' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{
      "project": "Kylin",
      "type": "user",
      "permission": "QUERY",
      "names":["test"]
      }'
  ```

- Response Example

  ```json
  {
      "code": "000",
      "data": "",
      "msg": ""
  }
  ```



###  Update Project ACL (overwrite) {#update-project-acl}

- `PUT http://host:port/kylin/api/access/project`

- Introduced in: 5.0

- HTTP Body: JSON Object
  - `project` - `required` `string`, project name.
  - `type` - `required` `string`, represents users or user groups, optional values are `user` or `group`.
  - `permission` - `required` `string`, represents users or user groups permission, optional values are `QUERY`, `OPERATION`,  `MANAGEMENT` and  `ADMIN`.
  - `name` - `required` `string`, name of user or user group.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X PUT \
     'http://host:port/kylin/api/access/project' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{
      "project": "Kylin",
      "type": "user",
      "permission": "ADMIN",
      "name": "test"
      }'
  ```

- Response Example

  ```json
  {
      "code": "000",
      "data": "",
      "msg": ""
  }
  ```



###  Revoke Project ACL {#revoke-project-acl}

- `DELETE http://host:port/kylin/api/access/project`

- Introduced in: 5.0

- Request Parameters

  - `project` - `required` `string`, project name. 
  - `type` - `required` `string`,  Represents users or user groups, optional are `user` or `group`.
  - `name` - `required` `string`, name of user or user group.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X DELETE \
     'http://host:port/kylin/api/access/project?project=learn_kylin&type=user&name=test' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Example

  ```json
  {
      "code": "000",
      "data": "",
      "msg": ""
  }
  ```



