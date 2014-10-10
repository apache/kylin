KylinApp.factory('CubeService', ['$resource', function ($resource, config) {
    return $resource(Config.service.url + 'cubes/:cubeId/:propName/:propValue/:action', {}, {
        list: {method: 'GET', params: {}, isArray: true},
        getSql: {method: 'GET', params: {propName: 'segs', action: 'sql'}, isArray: false},
        updateNotifyList: {method: 'PUT', params: {propName: 'notify_list'}, isArray: false},
        cost: {method: 'PUT', params: {action: 'cost'}, isArray: false},
        rebuildLookUp: {method: 'PUT', params: {propName: 'segs', action: 'refresh_lookup'}, isArray: false},
        rebuildCube: {method: 'PUT', params: {action: 'rebuild'}, isArray: false},
        disable: {method: 'PUT', params: {action: 'disable'}, isArray: false},
        enable: {method: 'PUT', params: {action: 'enable'}, isArray: false},
        drop: {method: 'DELETE', params: {}, isArray: false},
        save: {method: 'POST', params: {}, isArray: false},
        update: {method: 'PUT', params: {}, isArray: false},
        getHbaseInfo: {method: 'GET', params: {propName: 'hbase'}, isArray: true}
    });
}]);