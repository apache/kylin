/*
 Multiple-select directive for AngularJS
 (c) 2013 Alec LaLonde (https://github.com/alalonde/angular-multi-select)
 License: MIT
 */
(function(angular) {
    'use strict';

    angular.module('multi-select', [])
        .directive('multiSelect', ['$q', '$parse', function($q, $parse) {

            function appendSelected(entities) {
                var newEntities = [];
                angular.forEach(entities, function(entity) {
                    var appended = entity;
                    appended.selected = false;
                    newEntities.push(appended);
                });
                return newEntities;
            }

            return {
                restrict: 'E',
                require: 'ngModel',
                scope: {
                    selectedLabel: "@",
                    availableLabel: "@",
                    available: "=",
                    model: "=ngModel",
                    config: "="
                },
                templateUrl: "partials/directives/multiSelect.html",
                link: function(scope, elm, attrs, controllers) {
                    scope.selected = {
                        available: [],
                        current: []
                    };

                    /* Filters out items in original that are also in toFilter. Compares by reference. */
                    function filterOut(original, toFilter) {
                        var filtered = [];
                        angular.forEach(original, function(entity) {
                            var match = false;
                            for(var i = 0; i < toFilter.length; i++) {
                                if(scope.renderItem(toFilter[i]) == scope.renderItem(entity)) {
                                    match = true;
                                    break;
                                }
                            }
                            if(!match) {
                                filtered.push(entity);
                            }
                        });
                        return filtered;
                    }

                    function parseExpression(item, expr) {
                        var displayComponents = expr.match(/(.+)\s+as\s+(.+)/);
                        var ctx = {};
                        ctx[displayComponents[1]] = item;
                        return $parse(displayComponents[2])(ctx);
                    }

                    var requiredMin, inputModel;
                    function ensureMinSelected() {
                        if(requiredMin && scope.model) {
                            scope.numSelected = scope.model.length;
                            inputModel.$setValidity('min', scope.numSelected >= requiredMin);
                        }
                    }

                    scope.refreshAvailable = function() {
                        if(scope.model && scope.available){
                            scope.available = filterOut(scope.available, scope.model);
                            scope.selected.available = appendSelected(scope.available);
                            scope.selected.current = appendSelected(scope.model);
                        }
                    };

                    scope.add = function() {
                        if(!scope.model.length)
                            scope.model = [];
                        scope.model = scope.model.concat(scope.selected(scope.selected.available));
                    };
                    scope.remove = function() {
                        var selected = scope.selected(scope.selected.current);
                        scope.available = scope.available.concat(selected);
                        scope.model = filterOut(scope.model, selected);
                    };
                    scope.selected = function(list) {
                        var found = [];
                        angular.forEach(list, function(item) { if(item.selected === true) found.push(item); });
                        return found;
                    };

                    //Watching the model, updating if the model is a resolved promise
                    scope.watchModel = function(){
                        if(scope.model && scope.model.hasOwnProperty('$promise') && !scope.model.$resolved){
                            scope.model.then(function(results) {
                                scope.$watch('model', scope.watchModel);
                            });
                        }
                        else{
                            scope.refreshAvailable();
                            scope.$watch('model', scope.refreshAvailable);
                        }
                    };

                    //Watching the list of available items. Updating if it is a resolved promise, and refreshing the
                    //available list if the list has changed
                    var _oldAvailable = {};
                    scope.watchAvailable = function(){
                        if(scope.available && scope.available.hasOwnProperty('$promise') && !scope.available.$resolved){
                            scope.available.$promise.then(function(results) {
                                scope.$watch('available', scope.watchAvailable);
                            });
                        }
                        else{
                            //We only want to refresh the list if the list of available items has changed
                            //and the variable is defined
                            if(scope.available && scope.available != _oldAvailable){
                                scope.refreshAvailable();
                                _oldAvailable = scope.available;
                            }
                        }
                    };

                    scope.$watch("available", scope.watchAvailable);
                    scope.$watch("model", scope.watchModel);

                    scope.renderItem = function(item) {
                        return parseExpression(item, attrs.display);
                    };

                    scope.renderTitle = function(item) {
                        if(attrs.title) {
                            return parseExpression(item, attrs.title);
                        }
                        return "";
                    };

                    if(scope.config && angular.isDefined(scope.config.requiredMin)) {
                        var inputs = elm.find("input");
                        var validationInput = angular.element(inputs[inputs.length - 1]);
                        inputModel = validationInput.controller('ngModel');
                    }

                    scope.$watch('config.requiredMin', function(value) {
                        if(angular.isDefined(value)) {
                            requiredMin = parseInt(value, 10);
                            ensureMinSelected();
                        }
                    });

                    scope.$watch('model', function(selected) {
                        ensureMinSelected();
                    });
                }
            };
        }]);
})(angular);