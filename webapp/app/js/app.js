//Kylin Application Module
KylinApp = angular.module('kylin', ['ngRoute', 'ngResource', 'ngGrid', 'ui.bootstrap', 'ui.ace', 'base64', 'angularLocalStorage', 'localytics.directives', 'treeControl', 'nvd3ChartDirectives','ngRainbow','ngLoadingRequest']);
KylinApp.config(['rainbowBarProvider', function(rainbowBarProvider) {
    "use strict";
    rainbowBarProvider.configure({
        barThickness: 5
    });
}]);
