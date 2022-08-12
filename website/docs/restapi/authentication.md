---
title: Access and Authentication API
language: en
sidebar_label: Access and Authentication API
pagination_label: Access and Authentication API
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
  - access 
  - authentication
draft: false
last_update:
  date: 12/08/2022
---

### Access Information
The access prefix of all Kylin APIs is `/kylin/api`. This prefix is required regardless of which module API is accessed. For example, accessing model uses API of `/kylin/api/models`, and the correspondingly complete path is `http://host:port/kylin/api/models`.


### Authentication
All APIs in Kylin are based on [Basic Authentication](http://en.wikipedia.org/wiki/Basic_access_authentication) authentication mechanism. Basic Authentication is a simple access control mechanism, which encodes account and password information based on Base64. Adding these information as request headers to HTTP request commands, the back-end will decode the account and password information from the request header for authentication. Take the account and password `ADMIN:KYLIN` as an example, after encoding, the corresponding authentication information would be `Basic QURNSU46S1lMSU4=`, and the corresponding HTTP header information is `Authorization: Basic QURNSU46S1lMSU4=`. 



### Authentication Essentials
* Add `Authorization` to HTTP header
  * Or users could get authorized through `POST http://host:port/kylin/api/user/authentication`. Once the authentication passes, the authentication information would be stored in cookie files for the following visit. 


- HTTP Header
  - `Authorization:Basic QURNSU46S1lMSU4=`
  - `Accept: application/vnd.apache.kylin-v4-public+json`
  - `Accept-Language: en`
  - `Content-Type: application/json;charset=utf-8`

- Curl Request Example

  ```sh
  curl -X POST \
    'http://host:port/kylin/api/user/authentication' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Example

  ```json
    {
        "code":"000",
        "data":{
            "username":"ADMIN",
            "authorities":[{"authority": "ROLE_ADMIN"}, {"authority": "ALL_USERS"}],
            "disabled":false,
            "locked":false,
            "uuid":"0205dac6-215a-4454-84ae-3dcc85b9675c",
            "last_modified":1574756819619,
            "create_time":1563346648008,
            "version":"4.0.0.0",
            "mvcc":24,
            "locked_time":0,
            "wrong_time":2,
            "first_login_failed_time":1574756817981
        },
        "msg":""
    }
  ```



### JavaScript Authentication Request Example

> Note:  You can download "jquery.base64.js" at [https://github.com/yckart/jquery.base64.js](https://github.com/yckart/jquery.base64.js)

```javascript
var authorizationCode = $.base64('encode', 'NT_USERNAME' + ":" + 'NT_PASSWORD');
 
$.ajaxSetup({
   headers: { 
    'Authorization': "Basic " + authorizationCode, 
    'Content-Type': 'application/json;charset=utf-8',
    'Accept': 'application/vnd.apache.kylin-v4-public+json'
   }
});
```

```javascript
$.ajaxSetup({
      headers: { 
        'Authorization': "Basic eWFu**********X***ZA==",
        'Content-Type': 'application/json;charset=utf-8',
        'Accept': 'application/vnd.apache.kylin-v4-public+json'
      } // use your own authorization code here
    });
    var request = $.ajax({
       url: "http://host:port/kylin/api/query",
       type: "POST",
       data: '{"sql":"select count(*) from SUMMARY;","offset":0,"limit":50000,"acceptPartial":true,"project":"test"}',
       dataType: "json"
    });
    request.done(function( msg ) {
       alert(msg);
    }); 
    request.fail(function( jqXHR, textStatus ) {
       alert( "Request failed: " + textStatus );
  });
```

