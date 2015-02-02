KylinApp.factory('ModelService', ['$resource', function ($resource, config) {
    return $resource(Config.service.url + 'model/:model_name/:propName/:propValue/:action', {}, {
        get: {method: 'GET', params: {}, isArray: false}
    });
}])