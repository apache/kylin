KylinApp.factory('QueryService', ['$resource', function ($resource, config) {
    return $resource(Config.service.url + ':subject/:subject_id/:propName/:propValue/:action', {}, {
        query: {method: 'POST', params: {action: 'query'}, isArray: false},
        save: {method: 'POST', params: {subject: 'saved_queries'}, isArray: false},
        delete: {method: 'DELETE', params: {subject: 'saved_queries'}, isArray: false},
        list: {method: 'GET', params: {subject: 'saved_queries'}, isArray: true},
        export: {method: 'GET', params: {subject: 'query', propName: 'format', propValue: 'csv'}, isArray: false},
        getTables: {method: 'GET', params: {subject: 'tables_and_columns'}, isArray: true}
    });
}])
;