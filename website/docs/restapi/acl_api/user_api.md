---
title: User Management API
language: en
sidebar_label: User Management API
pagination_label: User Management API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - user management api
draft: false
last_update:
    date: 08/12/2022
---

> Reminders:
>
> 1. Please read [Access and Authentication REST API](../authentication.md) and understand how authentication works.
> 2. On Curl command line, don't forget to quote the URL if it contains the special char `&`.



###  Get User List {#get-user-list}

- `GET http://host:port/kylin/api/user`

- Introduced in: 4.1.4

- Request Parameters

  - `name` - `optional` `string`, username.
  - `is_case_sensitive` -  `optional` `boolean`, whether case sensitive on user name and  `false` is by default.
  - `page_offset` - `optional` `int`, offset of returned result, 0 by default.
  - `page_size` - `optional` `int`, quantity of returned result per page, 10 by default.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://localhost:7070/kylin/api/user' \
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
                  "username": "test",
                  "authorities": [
                      {
                          "authority": "ALL_USERS"
                      }
                    	...
                  ],
                  "disabled": false,
                  "default_password": false,
                  "locked": false,
                  "uuid": "af11440a-8e9d-4801-8508-5d0ce0e04a2f",
                  "last_modified": 1592296833935,
                  "create_time": 1592296833844,
                  "locked_time": 0,
                  "wrong_time": 0,
                  "first_login_failed_time": 0
              }
            	...
          ],
          "offset": 0,
          "limit": 10,
          "total_size": 3
      },
      "msg": ""
  }
  ```



###  Create User {#create-user}

- `POST http://host:port/kylin/api/user`

- Introduced in: 4.1.4

- HTTP Body: JSON Object

  - `username` - `required` `string`, username.
  - `password` - `required` `string`, password.
  - `disabled` - `required` `boolean`, enable the user or not, `true` means the user is disabled and `false` means the user is enabled.
  - `authorities` - `required` `array[string]`, the user belongs which user group.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X POST \
    'http://localhost:7070/kylin/api/user' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{
  	"username": "test",
  	"password": "Password@123",
  	"disabled": "false",
  	"authorities":["ALL_USERS"]
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



### Modify the group where user belongs {#modify-user}

**Note:** Modifying a user will overwrite the user group values.

- `PUT http://host:port/kylin/api/user`

- Introduced in: 4.1.4

- HTTP Body: JSON Object

  - `username` - `required` `string`, username.
  - `authorities` - `required` `array[string]`, the user belongs which user group.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X PUT \
    'http://localhost:7070/kylin/api/user' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{
  	"username": "createuser",
  	"authorities":["ALL_USERS"]
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



### Change Password {#change-password}

- `PUT http://host:port/kylin/api/user/password`

- Introduced in: 4.1.4

- HTTP Body: JSON Object

  - `username` - `required` `string`, username.
  - `password` - `required` `string`, original password.
  - `new_password` - `required` `string`, new password.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X PUT \
    'http://localhost:7070/kylin/api/user' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{
      "username": "test",
      "password": "Password@123",
      "new_password": "Password@1234"
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



### Delete User{#delete-user}

### {#delete-user}

- `DELETE http://host:port/kylin/api/user`

- Introduced in: 4.2.1

- HTTP Body: JSON Object

  - `username` - `required` `string`, username.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X DELETE \
    'http://localhost:7070/kylin/api/user' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
    -d '{
  	"username": "testuser"
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



###  Get ACL of a User or Group {#get-acl-for-user-or-group}

- `GET http://host:port/kylin/api/access/acls`

- Introduced in: 4.1.4

- Request Parameters

  - `type` - `required` `string`, represents users or user groups, optional values are `user` or `group`.
  - `name` - `required` `string`, user or group name.
  - `project` - `optional` `string`, project name, fill in the value to get all the permissions of a user or user group in the specified project.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://localhost:7070/kylin/api/access/acls?type=user&name=ADMIN' \
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
              "project_name": "test",
              "project_permission": "ADMIN",
              "acl_info": [
                  {
                      "tables": [
                          {
                              "authorized": true,
                              "columns": [
                                  {
                                      "authorized": true,
                                      "column_name": "C_ADDRESS"
                                  }
                                	...
                              ],
                              "rows": [],
                              "table_name": "CUSTOMER",
                              "authorized_column_num": 8,
                              "total_column_num": 8
                          }
                        	...
                      ],
                      "authorized_table_num": 6,
                      "total_table_num": 6,
                      "database_name": "SSB"
                  }
                	...
              ]
          }
        	...
      ],
      "msg": ""
  }
  ```
