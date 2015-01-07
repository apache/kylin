/*jshint undef: false, unused: false, indent: 2*/
/*global angular: false */


'use strict';

angular.module('demoApp').controller('NewCardController', ['$scope', '$modalInstance', 'column', function ($scope, $modalInstance, column) {

  function initScope(scope) {
    scope.columnName = column.name;
    scope.column = column;
    scope.title = '';
    scope.details = '';
  }

  $scope.addNewCard = function () {
    if (!this.newCardForm.$valid) {
      return false;
    }
    $modalInstance.close({title: this.title, column: column, details: this.details});
  };

  $scope.close = function () {
    $modalInstance.close();
  };

  initScope($scope);

}]);

