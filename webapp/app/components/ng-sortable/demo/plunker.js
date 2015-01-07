'use strict'

var app = angular.module('plunker', ['ui.sortable']);

app.controller('MainCtrl', function ($scope) {

  $scope.items = [{
    name: 'item 1'
  }, {
    name: 'item 2'
  }, {
    name: 'item 3'
  }, {
    name: 'item 4'
  }, {
    name: 'item 5'
  }, {
    name: 'item 6'
  }, {
    name: 'item 7'
  }, {
    name: 'item 8'
  }]

  $scope.sortableOptions = {
    containment: '#sortable-container'
  };
});