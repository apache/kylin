KylinApp.factory('JobService', ['$resource', function ($resource, config) {
    return $resource(Config.service.url + 'jobs/:jobId/:propName/:propValue/:action', {}, {
        list: {method: 'GET', params: {}, isArray: true},
        get: {method: 'GET', params: {}, isArray: false},
        stepOutput: {method: 'GET', params: {propName: 'steps', action: 'output'}, isArray: false},
        resume: {method: 'PUT', params: {action: 'resume'}, isArray: false},
        cancel: {method: 'PUT', params: {action: 'cancel'}, isArray: false}
    });
}]);