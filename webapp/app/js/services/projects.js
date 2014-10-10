KylinApp.factory('ProjectService', ['$resource', function ($resource, config) {
    return $resource(Config.service.url + 'projects/:projecId/:propName/:propValue/:action', {}, {
        list: {method: 'GET', params: {}, isArray: true},
        save: {method: 'POST', params: {}, isArray: false},
        update: {method: 'PUT', params:{}, isArray: false},
        delete: {method: 'DELETE', params: {}, isArray: false }
    });
}]);