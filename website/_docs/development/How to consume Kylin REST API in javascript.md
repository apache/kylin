Kylin securicty authorization is based on basic access authorization, so when you want to use API in your javascript,you need to involve the authorization info in http headers.


## Here is a demo show how to use Kylin Query API.
```
$.ajaxSetup({
      headers: { 'Authorization': "Basic eWFu**********X***ZA==", 'Content-Type': 'application/json;charset=utf-8' } // use your own authorization code here
    });
    var request = $.ajax({
       url: "http://hostname/kylin/api/query",
       type: "POST",
       data: '{"sql":"select count(*) from SUMMARY;","offset":0,"limit":50000,"partial":true,"project":"test"}',
       dataType: "json"
    });
    request.done(function( msg ) {
       alert(msg);
    }); 
    request.fail(function( jqXHR, textStatus ) {
       alert( "Request failed: " + textStatus );
  });

```


For what is basic access authorization ,click here http://en.wikipedia.org/wiki/Basic_access_authentication

## Keypoint:
1. add basic access authorization info in heders.
2. use right ajax type and right data synax

How to generate your authorization code (download and import "jquery.base64.js" from https://github.com/yckart/jquery.base64.js)

```
var authorizationCode = $.base64('encode', 'NT_USERNAME' + ":" + 'NT_PASSWORD');
 
$.ajaxSetup({
   headers: { 
    'Authorization': "Basic " + authorizationCode, 
    'Content-Type': 'application/json;charset=utf-8' 
   }
});
```