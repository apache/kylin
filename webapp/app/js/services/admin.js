/**
 * Created by jianliu on 2014/7/11.
 */
KylinApp.factory('AdminService', ['$resource', function ($resource, config) {
    return $resource(Config.service.url + 'admin/:action', {}, {
        env: {method: 'GET', params: {action: 'env'}, isArray: false},
        config: {method: 'GET', params: {action: 'config'}, isArray: false},
        cleanStorage: {method: 'DELETE', params:{action: 'storage'}, isArray: false},
        updateConfig: {method: 'PUT', params:{action: 'config'}, isArray:false}
    });
}]);