# angular-sweetalert ![](http://img.shields.io/badge/bower_module-v1.3.0-green.svg) #
==================

An angular service which expose sweetalert in angular way.

## Requirements ##

- [angular][angular-url]
- [sweetalert][sweetalert-url]

> Since [sweetalert][sweetalert-url] import [google-fonts][google-fonts-url], developer in China may found issue while loading the [sweet-alert.css](https://github.com/t4t5/sweetalert/blob/master/lib/sweet-alert.css). Just delete the first line from it to solve the problem or download the fonts.

## Install ##

```powershell
bower install sweetalert --save
bower install angular-h-sweetalert --save
```

## Import ##

```html
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>DEMO</title>
    <link rel="stylesheet" type="text/css" href="libs/sweet-alert.css">
</head>
<body>
    <script type="text/javascript" src="libs/sweet-alert.min.js"></script>
    <script type="text/javascript" src="libs/angular.min.js"></script>
    <script type="text/javascript" src="ngSweetAlert.js"></script>
</body>
</html>
```

## Usage ##

```javascript

var demo = angular.module('demo', ['hSweetAlert']);

demo.controller('demoController', function($scope, sweet) {
    $scope.basic = function() {
        sweet.show('Simple right?');
    };
});
```

See full featured demo: http://leftstick.github.io/angular-sweetalert/



## LICENSE ##

[MIT License](https://raw.githubusercontent.com/leftstick/angular-sweetalert/master/LICENSE)

[angular-url]: https://angularjs.org/
[sweetalert-url]: http://tristanedwards.me/sweetalert
[google-fonts-url]: http://fonts.googleapis.com/css?family=Open+Sans:400,600,700,300