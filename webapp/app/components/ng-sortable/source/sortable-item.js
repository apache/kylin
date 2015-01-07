/*jshint indent: 2 */
/*global angular: false */

(function () {

  'use strict';
  var mainModule = angular.module('ui.sortable');

  /**
   * Controller for sortable item.
   *
   * @param $scope - drag item scope
   */
  mainModule.controller('ui.sortable.sortableItemController', ['$scope', function ($scope) {

    this.scope = $scope;

    $scope.sortableScope = null;
    $scope.modelValue = null; // sortable item.
    $scope.type = 'item';

    /**
     * returns the index of the drag item from the sortable list.
     *
     * @returns {*} - index value.
     */
    $scope.index = function () {
      return $scope.sortableScope.modelValue.indexOf($scope.modelValue);
    };

    /**
     * Returns the item model data.
     *
     * @returns {*} - item model value.
     */
    $scope.itemData = function () {
      return $scope.sortableScope.modelValue[$scope.$index];
    };

  }]);

  /**
   * sortableItem directive.
   */
  mainModule.directive('asSortableItem', ['sortableConfig',
    function (sortableConfig) {
      return {
        require: '^asSortable',
        restrict: 'A',
        controller: 'ui.sortable.sortableItemController',
        link: function (scope, element, attrs, sortableController) {

          if (sortableConfig.itemClass) {
            element.addClass(sortableConfig.itemClass);
          }
          scope.sortableScope = sortableController.scope;
          scope.modelValue = sortableController.scope.modelValue[scope.$index];
          scope.element = element;
        }
      };
    }]);

}());