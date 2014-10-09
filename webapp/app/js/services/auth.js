KylinApp.factory('AuthenticationService', ['$resource', function ($resource, config) {
    return $resource(Config.service.url + 'user/authentication/:action', {}, {
        ping: {method: 'GET', params: {}, isArray: false},
        login: {method: 'POST', params: {}, isArray: false},
        authorities: {method: 'GET', params: {action: 'authorities'}, cache: true, isArray: false}
    });
}]);
