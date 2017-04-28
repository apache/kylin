/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// # dh.config
//
// This module loads the configuration and routes files, as well as bootstraps the application. At
// runtime it adds uri based on application location.

// Config variable.
var Config = {
  name: 'kylin',
  service: {
    base: '/kylin/',
    url: '/kylin/api/'
  },
  documents: [],
  reference_links: {
    hadoop: {name: "hadoop", link: null},
    diagnostic: {name: "diagnostic", "link": null}
  },
  contact_mail: ''
};
//resolve startsWith and endsWidth not work in low version chrome
if (typeof String.prototype.startsWith != 'function') {
  String.prototype.startsWith = function (prefix){
    return this.slice(0, prefix.length) === prefix;
  };
}
if (typeof String.prototype.endsWith != 'function') {
  String.prototype.endsWith = function(suffix) {
    return this.indexOf(suffix, this.length - suffix.length) !== -1;
  };
}
// Angular module to load routes.
KylinApp.config(function ($routeProvider, $httpProvider, $locationProvider, $logProvider) {
    //resolve http always use cache data in IE11,IE10
    $httpProvider.defaults.headers.common['Cache-Control'] = 'no-cache';
    $httpProvider.defaults.headers.common['Pragma'] = 'no-cache';
    // Set debug to true by default.
    if (angular.isUndefined(Config.debug) || Config.debug !== false) {
      Config.debug = true;
    }

    // Set development to true by default.
    if (angular.isUndefined(Config.development) || Config.development !== false) {
      Config.development = true;
    }

    // Disable logging if debug is off.
    if (Config.debug === false) {
      $logProvider.debugEnabled(false);
    }

    // Loop over routes and add to router.
    angular.forEach(Config.routes, function (route) {
      $routeProvider.when(route.url, route.params).otherwise({redirectTo: '/'});
    });

    // Set to use HTML5 mode, which removes the #! from modern browsers.
    $locationProvider.html5Mode(true);

    //configure $http to view a login whenever a 401 unauthorized response arrives
    $httpProvider.responseInterceptors.push(function ($rootScope, $q) {
      return function (promise) {
        return promise.then(
          //success -> don't intercept
          function (response) {
            return response;
          },
          //error -> if 401 save the request and broadcast an event
          function (response) {
            if (response.status === 401 && !(response.config.url.indexOf("user/authentication") !== -1 && response.config.method === 'POST')) {
              var deferred = $q.defer(),
                req = {
                  config: response.config,
                  deferred: deferred
                };
              $rootScope.requests401.push(req);
              $rootScope.$broadcast('event:loginRequired');
              return deferred.promise;
            }

            if (response.status === 403) {
              $rootScope.$broadcast('event:forbidden', response.data.exception);
            }

            return $q.reject(response);
          }
        );
      };
    });
    httpHeaders = $httpProvider.defaults.headers;
  })
  .run(function ($location) {

    if (angular.isUndefined(Config.uri)) {
      Config.uri = {};
    }

    // Add uri details at runtime based on environment.
    var uri = {
      host: $location.protocol() + '://' + $location.host() + '/'
    };
    // Setup values for development or production.
    if (Config.development) {
      uri.api = $location.protocol() + '://' + $location.host() + '/devapi/';
    } else {
      uri.api = $location.protocol() + '://' + $location.host() + '/api/';
    }

    // Extend uri config with any declared uri values.
    Config.uri = angular.extend(uri, Config.uri);
  });

// This runs when all code has loaded, and loads the config and route json manifests, before bootstrapping angular.




window.onload = function () {

  // Files to load initially.
  var files = [
    {property: 'routes', file: 'routes.json'}
  ];
  var loaded = 0;

  // Request object
  var Request = function (item, file) {
    var loader = new XMLHttpRequest();
    // onload event for when the file is loaded
    loader.onload = function () {

      loaded++;

      if (item === 'routes') {
        Config[item] = JSON.parse(this.responseText);
      }
      // We've loaded all dependencies, lets bootstrap the application.
      if (loaded === files.length) {
        // Declare error if we are missing a name.
        if (angular.isUndefined(Config.name)) {
          console.error('Config.name is undefined, please update this property.');
        }
        // Bootstrap the application.
        angular.bootstrap(document, [Config.name]);
      }
    };

    loader.open('get', file, true);
    loader.send();
  };

  for (var index in files) {
    var load = new Request(files[index].property, files[index].file);
  }

};
