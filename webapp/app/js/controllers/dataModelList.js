'use strict';

KylinApp.controller('DataModelListCtrl', function ($scope, $sce) {

    var DataModel = {
        init: function () {
            return {
                "name": "",
                "fact_table": "",
                "lookups": []
            }
        }
    };

    // Initialize params.
    $scope.modelState = {
        editing: false,
        editingIndex: -1,
        showPanel: false,
        filter: ''
    };

    $scope.newModel = DataModel.init();

    // Mock the list data.
    var dataModelList = $scope.dataModelList = [
        {
            "name": "some",
            "fact_table": "TEST_CAL_DT",
            "lookups": [
                {
                    "table": "TEST_CAL_DT",
                    "join": {
                        "type": "inner",
                        "primary_key": [
                            "CAL_DT",
                            "YEAR_BEG_DT"
                        ],
                        "foreign_key": [
                            "CAL_DT",
                            "YEAR_BEG_DT"
                        ]
                    }
                },
                {
                    "table": "TEST_CAL_DT",
                    "join": {
                        "type": "inner",
                        "primary_key": [
                            "CAL_DT",
                            "YEAR_BEG_DT"
                        ],
                        "foreign_key": [
                            "CAL_DT",
                            "YEAR_BEG_DT"
                        ]
                    }
                }
            ]
        }
    ];

    $scope.$watch('dataModelList', function (newValue, oldValue) {
        // This prevents unneeded calls to the storage.
        if(newValue !== oldValue) {
            console.dir(dataModelList);
        }
    }, true);

    $scope.addDataModel = function () {
        // Push newModel which bound user input data.
        dataModelList.push(angular.copy($scope.newModel));

        $scope.resetParams();
    };

    $scope.editDataModel = function (model) {
        $scope.modelState.editingIndex = dataModelList.indexOf(model);
        $scope.modelState.editing = true;

        // Make a copy of model will be editing.
        $scope.newModel = angular.copy(model);

        $scope.modelState.showPanel = true;
    };

    $scope.doneEditDataModel = function () {
        // Copy edited model to destination model.
        angular.copy($scope.newModel, dataModelList[$scope.modelState.editingIndex]);

        $scope.resetParams();
    };

    $scope.cancelDataModel = function () {
        $scope.resetParams();
    };

    $scope.removeDataModel = function (model) {
        dataModelList.splice(dataModelList.indexOf(model), 1);
    };

    $scope.resetParams = function () {
        $scope.modelState.editing = false;
        $scope.modelState.editingIndex = -1;
        $scope.modelState.showPanel = false;

        $scope.newModel = DataModel.init();
    };

});
