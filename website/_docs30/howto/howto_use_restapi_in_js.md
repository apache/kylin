---
layout: docs
title:  Use RESTful API in Javascript
categories: howto
permalink: /docs/howto/howto_use_restapi_in_js.html
---
Kylin security is based on basic access authorization, if you want to use API in your javascript, you need to add authorization info in http headers.

## Example on Query API.
```
$.ajaxSetup({
      headers: { 'Authorization': "Basic eWFu**********X***ZA==", 'Content-Type': 'application/json;charset=utf-8' } // use your own authorization code here
    });
    var request = $.ajax({
       url: "http://hostname/kylin/api/query",
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

## Keypoints
1. add basic access authorization info in http headers.
2. use right ajax type and data synax.

## Basic access authorization
For what is basic access authorization, refer to [Wikipedia Page](http://en.wikipedia.org/wiki/Basic_access_authentication).
How to generate your authorization code (download and import "jquery.base64.js" from [https://github.com/yckart/jquery.base64.js](https://github.com/yckart/jquery.base64.js)).

```
var authorizationCode = $.base64('encode', 'NT_USERNAME' + ":" + 'NT_PASSWORD');
 
$.ajaxSetup({
   headers: { 
    'Authorization': "Basic " + authorizationCode, 
    'Content-Type': 'application/json;charset=utf-8' 
   }
});
```
