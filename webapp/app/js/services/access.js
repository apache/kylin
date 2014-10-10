KylinApp.factory('AccessService', ['$resource', function ($resource, config) {
    return $resource(Config.service.url + 'access/:type/:uuid', {}, {
        list: {method: 'GET', params: {}, isArray: true},
        grant: {method: 'POST', params: {}, isArray: true},
        update: {method: 'PUT', params: {}, isArray: true},
        revoke: {method: 'DELETE', params: {}, isArray: false}
    });
}]);