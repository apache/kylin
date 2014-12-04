;(function(global, angular) {
    'use strict';

    var sweet = angular.module('hSweetAlert', []);

    var al = global.swal;

    var service = function($rootScope) {

        this.show = function() {
            var args = [].slice.call(arguments, 0);
            al.apply(undefined, args);
        };
    };

    sweet.service('sweet', ['$rootScope', service]);

}(window, angular));