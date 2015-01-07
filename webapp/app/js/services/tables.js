KylinApp.factory('TableService', ['$resource', function ($resource, config) {
    return $resource(Config.service.url + 'tables/:tableName/:action', {}, {
        list: {method: 'GET', params: {}, cache: true, isArray: true},
        get: {method: 'GET', params: {}, isArray: false},
        getExd: {method: 'GET', params: {action: 'exd-map'}, isArray: false},
        reload: {method: 'PUT', params: {action: 'reload'}, isArray: false},
        loadHiveTable: {method: 'POST', params: {}, isArray: false},
        genCardinality: {method: 'PUT', params: {action: 'cardinality'}, isArray: false}
    });
}]);