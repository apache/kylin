KylinApp.factory('CubeDescService', ['$resource', function ($resource, config) {
    return $resource(Config.service.url + 'cube_desc/:cube_name/:propName/:propValue/:action', {}, {
        get: {method: 'GET', params: {}, isArray: true}
    });
}])