# An AngularJS module adapting underscore

This module exposes underscore's API into angular app's root scope,
and provides some filters from underscore.


## Filters

Whole Underscore's API for Collections, Arrays and Objects except decision API
(e.g. functions return true|false), side effect guys, and _.range(not making sense as a filter).


For API details please check out http://underscorejs.org/

## How to use

### Install

After load angular.js and underscore.js:

```html
<script type="text/javascript" src="angular-underscore.js"></script>
```

### Load angular-underscore

#### Load whole stuff

```javascript
angular.module('yourAwesomeApp', ['angular-underscore']);
```

#### Load API only

```javascript
angular.module('yourAwesomeApp', ['angular-underscore/utils']);
```

#### Load filters only

```javascript
angular.module('yourAwesomeApp', ['angular-underscore/filters']);
```

#### Load specific feature only

```javascript
// load `shuffle` only
angular.module('yourAwesomeApp', ['angular-underscore/filters/shuffle']);
```

### Usecase

#### From the Template

```html
<script type="text/javascript">
  angular.module('example', ['angular-underscore']);
</script>

<body ng-app="example">
  <!-- generate 10 unduplicated random number from 0 to 9 -->
  <div ng-repeat="num in range(10)|shuffle">{{num}}</div>
</body>
```

#### From the Controller

```javascript
angular.module('yourAwesomeApp', ['angular-underscore'])
.controller('yourAwesomeCtrl', function($scope) {
    $scope.sample([1, 2, 3]); // got 1, or 2, or 3.
    $scope._([1, 2, 3]); // underscore's chain, http://underscorejs.org/#chain
});

```

### Local build

```
$ npm install uglify-js -g
$ uglifyjs angular-underscore.js > angular-underscore.min.js
```

## License

(The MIT License)

Copyright (c) 2014 <floydsoft@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the 'Software'), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
