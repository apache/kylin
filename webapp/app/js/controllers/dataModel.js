'use strict';

KylinApp.controller('DataModelCtrl', function ($scope, $sce, TableService) {

    $scope.joinTypes = [
        {name: 'Left', value: 'left'},
        {name: 'Inner', value: 'inner'},
        {name: 'Right', value: 'right'}
    ];

    var Lookup = {
        init: function () {
            return {
                "table": "",
                "join": {
                    "type": "",
                    "primary_key": [],
                    "foreign_key": []
                }
            };
        }
    };

    // Check the mode - edit or view.
    if (angular.isUndefined($scope.modelState.mode)) {
        $scope.modelState.mode = 'edit';
    }

    // Initialize params.
    $scope.lookupState = {
        editing: false,
        editingIndex: -1,
        showPanel: false,
        filter: ''
    };

    $scope.newLookup = Lookup.init();

    $scope.srcTablesInProject = [];

    $scope.$watch('project.selectedProject', function (newValue, oldValue) {
        if (!newValue) {
            return;
        }

        $scope.srcTablesInProject = [];
        var param = {
            ext: true,
            project:newValue
        };
        if (newValue) {
            TableService.list(param, function (tables) {
                angular.forEach(tables, function (table) {
                    $scope.srcTablesInProject.push(table);
                });
            });
        }
    });

    $scope.getColumnsByTable = function (name) {
        var temp = null;
        angular.forEach($scope.srcTablesInProject, function (table) {
            if (table.name == name) {
                temp = table.columns;
            }
        });
        return temp;
    };

    var lookupList = $scope.newModel.lookups;

    $scope.addLookup = function () {
        // Push newLookup which bound user input data.
        lookupList.push(angular.copy($scope.newLookup));

        $scope.resetParams();
    };

    $scope.editLookup = function (lookup) {
        $scope.lookupState.editingIndex = lookupList.indexOf(lookup);
        $scope.lookupState.editing = true;

        // Make a copy of model will be editing.
        $scope.newLookup = angular.copy(lookup);

        $scope.lookupState.showPanel = true;
    };

    $scope.doneEditLookup = function () {
        // Copy edited model to destination model.
        angular.copy($scope.newLookup, lookupList[$scope.lookupState.editingIndex]);

        $scope.resetParams();
    };

    $scope.cancelLookup = function () {
        $scope.resetParams();
    };

    $scope.removeLookup = function (lookup) {
        lookupList.splice(lookupList.indexOf(lookup), 1);
    };

    $scope.resetParams = function () {
        $scope.lookupState.editing = false;
        $scope.lookupState.editingIndex = -1;
        $scope.lookupState.showPanel = false;

        $scope.newLookup = Lookup.init();
    };

    $scope.removeElement = function (arr, element) {
        var index = arr.indexOf(element);

        if (index > -1) {
            arr.splice(index, 1);
        }
    };

});
