'use strict'

var app = angular.module('scrollableContainer', ['ui.sortable']);

app.controller('MainCtrl', function ($scope) {

    $scope.items1 = [
        {
            name: 'item 1'
        }, {
            name: 'item 2'
        }
    ];

    $scope.items2 = [
    {
        name: 'item 1'
    }, {
        name: 'item 2'
    }
    ];

    $scope.sortableOptions1 = {
        containment: '#sortable-container1'
    };

    $scope.sortableOptions2 = {
        containment: '#sortable-container2',
        containerPositioning: 'relative'
    };
});