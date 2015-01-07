// # dh.config
//
// This module loads the configuration and routes files, as well as bootstraps the application. At 
// runtime it adds uri based on application location.

// Config variable.
var Config = {};

// Angular module to load routes.
KylinApp.config(function ($routeProvider, $httpProvider, $locationProvider, $logProvider) {

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
        $routeProvider.when(route.url, route.params);
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

//                    if (response.status === 500) {
//                        $rootScope.$broadcast('event:error', response.data.exception);
//                    }

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
        {property: 'config', file: 'config.json'},
        {property: 'routes', file: 'routes.json'}
    ];
    var loaded = 0;

    // Request object
    var Request = function (item, file) {
        var loader = new XMLHttpRequest();
        // onload event for when the file is loaded
        loader.onload = function () {

            loaded++;

            if (item === 'config') {
                Config = angular.extend(Config, JSON.parse(this.responseText));
            } else {
                Config[item] = JSON.parse(this.responseText);
            }

            // We've loaded all dependencies, lets bootstrap the application.
            if (loaded === files.length) {
                // Declare error if we are missing a name.
                if (angular.isUndefined(Config.name)) {
                    console.error('Config.name is undefined, please update config.json to include this property.');
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