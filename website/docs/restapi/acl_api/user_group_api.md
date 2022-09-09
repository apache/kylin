---
title: User Group Management API
language: en
sidebar_label: User Group Management API
pagination_label: User Group Management API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - user group management api
draft: false
last_update:
    date: 08/12/2022
---


> Reminders:
>
> 1. Please read [Access and Authentication REST API](../authentication.md) and understand how authentication works.
> 2. On Curl command line,  don't forget to quote the URL if it contains `&` or other special chars.



###  Get User Group List {#get-user-group-list}

- `GET http://host:port/kylin/api/user_group/groups`

- Introduced in: 5.0

- Request Parameters

  - `group_name` - `optional` `string`, group name.
  - `is_case_sensitive` -  `optional` `boolean`, whether case-sensitive on user group name. The default value is ` false`.
  - `page_offset` - `optional` `int`, offset of returned result, 0 by default.
  - `page_size` - `optional` `int`, quantity of returned result per page, 10 by default.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/user_group/groups`' \
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
              "ALL_USERS", 
              "ROLE_ADMIN", 
              "ROLE_ANALYST", 
              "ROLE_MODELER"
            	...
          ], 
          "offset": 0, 
          "limit": 10, 
          "total_size": 7
      }, 
      "msg": ""
  }
  ```



###  Get User List of Specified User Group {#get-user-list-of-specified-user-group}

- `GET http://host:port/kylin/api/user_group/group_members/{group_name}`

- Introduced in: 5.0

- URL Parameters

  - `group_name` - `required` `string`, group name.

- Request Parameters

  - `username` - `optional` `string`, username.
  - `page_offset` - `optional` `int`, offset of returned result, 0 by default.
  - `page_size` - `optional` `int`, quantity of returned result per page, 10 by default.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X GET \
    'http://host:port/kylin/api/user_group/group_members/test' \
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
                  "username": "ADMIN",
                  "authorities": [
                      {
                          "authority": "ROLE_ADMIN"
                      },
                      {
                          "authority": "ALL_USERS"
                      }
                  ],
                  "disabled": false,
                  "default_password": false,
                  "locked": false,
                  "uuid": "aaf02c5d-1605-42fa-abf9-9b0bb5715a6a",
                  "last_modified": 1592555313558,
                  "create_time": 1586744927779,
                  "locked_time": 0,
                  "wrong_time": 0,
                  "first_login_failed_time": 0
              }
            	...
          ],
          "offset": 0,
          "limit": 10,
          "total_size": 10
      },
      "msg": ""
  }
  ```



### Add User Group {#add-user-group}

- `POST http://host:port/kylin/api/user_group`

- Introduced in: 5.0

- HTTP Body: JSON Object

  - `group_name` - `required` `string`, group name.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X POST \
    'http://host:port/kylin/api/user_group' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{
      "group_name": "test_group"
  	}'
  ```

- Response Example

  ```json
  {
      "code": "000", 
      "data": "", 
      "msg": "add user group"
  }
  ```



### Update User Group {#update-user-group}

**Note:** Updating group will overwrite the original user list.

- `PUT http://host:port/kylin/api/user_group/users`

- Introduced in: 5.0

- HTTP Body: JSON Object

  - `group_name` - `required` `string`, group name.
  - `users` - `required` `array[string]`, list of users in group.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X PUT \
    'http://host:port/kylin/api/user_group/users' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8' \
    -d '{
  	"group_name": "test", 
  	"users":["ANALYST",  "MODELER"]
  	}'
  ```

- Response Example

  ```json
  {
      "code": "000", 
      "data": "", 
      "msg": "modify users in user group"
  }
  ```


### Delete User Group {#delete-user-group}

- `DELETE http://host:port/kylin/api/user_group`

- Introduced in: 5.0

- HTTP Body: JSON Object

  - `group_name` - `required` `string`, group name.

- HTTP Header

  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X DELETE \
    'http://host:port/kylin/api/user_group' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
    -d '{
      "group_name": "test_group"
  	}'
  ```

- Response Example

  ```json
  {
      "code": "000", 
      "data": "", 
      "msg": "del user group"
  }
  ```

