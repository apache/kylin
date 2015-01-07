KylinApp.service('UserService', function ($http, $q) {
    var roles = {
        'ROLE_MODELER': '/cubes',
        'ROLE_ANALYST': '/cubes',
        'ROLE_ADMIN': '/cubes'
    };
    var curUser = {};

    this.getCurUser = function () {
        return curUser;
    };
    this.setCurUser = function (user) {
        curUser = user;
    };
    this.hasRole = function (role) {
        var hasRole = false;
        if (curUser.userDetails) {
            angular.forEach(curUser.userDetails.authorities, function (authority, index) {
                if (authority.authority == role) {
                    hasRole = true;
                }
            });
        }

        return hasRole;
    };
    this.isAuthorized = function () {
        return  curUser.userDetails && curUser.userDetails.authorities && curUser.userDetails.authorities.length > 0;
    };
    this.getHomePage = function () {
        var homePage = "/login";

        if (curUser.userDetails && curUser.userDetails.authorities) {
            angular.forEach(curUser.userDetails.authorities, function (authority, index) {
                homePage = (!!roles[authority.authority]) ? roles[authority.authority] : homePage;
            });
        }

        return homePage;
    }
});