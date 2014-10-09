KylinApp.factory('CacheService', ['$resource', function ($resource, config) {
    return $resource(Config.service.url + 'cache/:type/:name/:action', {}, {
        clean: {method: 'PUT', params: {type:'cube', name:'all', action: 'update'}, isArray: false}
    });
}]);