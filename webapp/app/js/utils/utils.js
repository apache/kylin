'use strict';

/* utils */

KylinApp.factory('VdmUtil', function ($modal, $timeout, $location, $anchorScroll, $window) {
    return {
        createDialog: function (template, scope, thenFunc, options) {
            options = (!!!options) ? {} : options;
            options = angular.extend({
                backdropFade: true,
                templateUrl: template,
                resolve: {
                    scope: function () {
                        return scope;
                    }
                },
                controller: function ($scope, $modalInstance, scope) {
                    $scope = angular.extend($scope, scope);
                    $scope.animate = Config.default.modalAnimateIn;
                    $scope.close = function (data) {
                        $scope.animate = Config.default.modalAnimateOut;
                        $timeout(function () {
                            $modalInstance.close(data);
                        }, 500);
                    }
                }
            }, options);

            var dialog = $modal.open(options);
            dialog.result.then(thenFunc);
        },

        formatDate: function (date, fmt) {
            var o = {
                "M+": date.getMonth() + 1,
                "d+": date.getDate(),
                "h+": date.getHours(),
                "m+": date.getMinutes(),
                "s+": date.getSeconds(),
                "q+": Math.floor((date.getMonth() + 3) / 3),
                "S": date.getMilliseconds()
            };
            if (/(y+)/.test(fmt)) fmt = fmt.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
            for (var k in o)
                if (new RegExp("(" + k + ")").test(fmt)) fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));

            return fmt;
        }
    }
});