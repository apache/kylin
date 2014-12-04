KylinApp.run(function ($rootScope, $http, $location, UserService, AuthenticationService, MessageService,sweet) {

    $rootScope.permissions = {
        READ: {name: 'CUBE QUERY', value: 'READ', mask: 1},
        MANAGEMENT: {name: 'CUBE EDIT', value: 'MANAGEMENT', mask: 32},
        OPERATION: {name: 'CUBE OPERATION', value: 'OPERATION', mask: 64},
        ADMINISTRATION: {name: 'CUBE ADMIN', value: 'ADMINISTRATION', mask: 16}
    };

    $rootScope.$on("$routeChangeStart", function () {
        AuthenticationService.ping(function (data) {
            UserService.setCurUser(data);
        });

        if ($location.url() == '' || $location.url() == '/') {
            AuthenticationService.ping(function (data) {
                UserService.setCurUser(data);
                $location.path(UserService.getHomePage());
            });
            return;
        }
    });

    /**
     * Holds all the requests which failed due to 401 response.
     */
    $rootScope.requests401 = [];

    $rootScope.$on('event:loginRequired', function () {
        $rootScope.requests401 = [];

        $location.path('/login');
    });

    /**
     * On 'event:loginConfirmed', resend all the 401 requests.
     */
    $rootScope.$on('event:loginConfirmed', function () {
        var i,
            requests = $rootScope.requests401,
            retry = function (req) {
                $http(req.config).then(function (response) {
                    req.deferred.resolve(response);
                });
            };

        for (i = 0; i < requests.length; i += 1) {
            retry(requests[i]);
        }
        $rootScope.requests401 = [];
    });

    /**
     * On 'logoutRequest' invoke logout on the server.
     */
    $rootScope.$on('event:logoutRequest', function () {
        httpHeaders.common['Authorization'] = null;
    });

    if ($location.url() == '' || $location.url() == '/') {
        AuthenticationService.ping(function (data) {
            UserService.setCurUser(data);
            $location.path(UserService.getHomePage());
        });
        return;
    }

    /**
     * On 'event:forbidden', resend all the 403 requests.
     */
    $rootScope.$on('event:forbidden', function (event, message) {
        var msg = !!(message) ? message : 'You don\' have right to take the action.';
//        MessageService.sendMsg('Permission Denied: ' + msg, 'error', {});
        sweet.show('Oops...', 'Permission Denied: ' + msg, 'error');

    });

    /**
     * On 'event:error', resend all the 500 requests.
     */
    $rootScope.$on('event:error', function (event, message) {
        var msg = !!(message) ? message : 'Failed to take action.';
        sweet.show('Oops...', 'Action Failed: ' + msg, 'error');
        MessageService.sendMsg('Action Failed: ' + msg, 'error', {});

    });
});