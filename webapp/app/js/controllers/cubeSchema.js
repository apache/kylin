'use strict';

KylinApp.controller('CubeSchemaCtrl', function ($scope, QueryService, UserService, ProjectService, AuthenticationService) {
    //~ Define metadata & class
    $scope.capacities = ['SMALL', 'MEDIUM', 'LARGE'];
    $scope.cubePartitionTypes = ['APPEND', 'UPDATE_INSERT'];
    $scope.projects = [];
    $scope.newDimension = null;
    $scope.newMeasure = null;

    $scope.wizardSteps = [
        {title: 'Cube Info', src: 'partials/cubeDesigner/info.html', isComplete: false},
        {title: 'Data Model', src: 'partials/cubeDesigner/data_model.html', isComplete: false},
        {title: 'Dimensions', src: 'partials/cubeDesigner/dimensions.html', isComplete: false},
        {title: 'Measures', src: 'partials/cubeDesigner/measures.html', isComplete: false},
        {title: 'Filter', src: 'partials/cubeDesigner/filter.html', isComplete: false},
        {title: 'Refresh Setting', src: 'partials/cubeDesigner/incremental.html', isComplete: false}
    ];
    if (UserService.hasRole("ROLE_ADMIN")) {
            $scope.wizardSteps.push({title: 'Advanced Setting', src: 'partials/cubeDesigner/advanced_settings.html', isComplete: false});
    }
    $scope.wizardSteps.push({title: 'Overview', src: 'partials/cubeDesigner/overview.html', isComplete: false});

    $scope.curStep = $scope.wizardSteps[0];

    var Measure = {
        createNew: function () {
            var measure = {
                "id": "",
                "name": "",
                "function": {
                    "expression": "",
                    "returntype": "",
                    "parameter": {
                        "type": "",
                        "value": ""
                    }
                }
            };

            return measure;
        }
    };

    // ~ init
    if (!$scope.state) {
        $scope.state = {mode: "view"};
    }

    $scope.$watch('cube.detail', function (newValue, oldValue) {
        if(!newValue){
            return;
        }
        if (newValue) {
            $scope.cubeMetaFrame = newValue;
        }
    });

    $scope.$watch('cubeMetaFrame', function (newValue, oldValue) {
        if(!newValue){
            return;
        }
        if ($scope.cubeMode=="editExistCube"&&newValue && !newValue.project) {
            initProject();
        }
        if($scope.cubeMetaFrame&&($scope.cubeMetaFrame.cube_partition_desc.partition_date_start||$scope.cubeMetaFrame.cube_partition_desc.partition_date_start==0))
        {
            $scope.cubeMetaFrame.cube_partition_desc.partition_date_start+=new Date().getTimezoneOffset()*git st
        ;
        }
        //convert from UTC to local timezone

    });

    // ~ public methods
    $scope.filterProj = function(project){
        return $scope.userService.hasRole('ROLE_ADMIN') || $scope.hasPermission(project,$scope.permissions.ADMINISTRATION.mask);
    };

    $scope.addNewMeasure = function (measure) {
        $scope.newMeasure = (!!measure)? measure:Measure.createNew();
    };

    $scope.clearNewMeasure = function () {
        $scope.newMeasure = null;
    };

    $scope.saveNewMeasure = function () {
        if ($scope.cubeMetaFrame.measures.indexOf($scope.newMeasure) === -1) {
            $scope.cubeMetaFrame.measures.push($scope.newMeasure);
        }
        $scope.newMeasure = null;
    };

    $scope.addNewRowkeyColumn = function () {
        $scope.cubeMetaFrame.rowkey.rowkey_columns.push({
            "column": "",
            "length": 0,
            "dictionary": "true",
            "mandatory": false
        });
    };

    $scope.addNewAggregationGroup = function () {
        $scope.cubeMetaFrame.rowkey.aggregation_groups.push([]);
    };

    $scope.refreshAggregationGroup = function (list, index, aggregation_groups) {
        if (aggregation_groups) {
            list[index] = aggregation_groups;
        }
    };

    $scope.removeElement = function (arr, element) {
        var index = arr.indexOf(element);
        if (index > -1) {
            arr.splice(index, 1);
        }
    };

    $scope.open = function ($event) {
        $event.preventDefault();
        $event.stopPropagation();

        $scope.opened = true;
    };

    $scope.preView = function () {
        var stepIndex = $scope.wizardSteps.indexOf($scope.curStep);
        if (stepIndex >= 1) {
            $scope.curStep.isComplete = false;
            $scope.curStep = $scope.wizardSteps[stepIndex - 1];
        }
    };

    $scope.nextView = function () {
        var stepIndex = $scope.wizardSteps.indexOf($scope.curStep);
        if (stepIndex < ($scope.wizardSteps.length - 1)) {
            $scope.curStep.isComplete = true;
            $scope.curStep = $scope.wizardSteps[stepIndex + 1];

            AuthenticationService.ping(function (data) {
                UserService.setCurUser(data);
            });
        }
    };

    // ~ private methods
    function initProject() {
        ProjectService.list({}, function (projects) {
            $scope.projects = projects;

            var cubeName = (!!$scope.routeParams.cubeName)? $scope.routeParams.cubeName:$scope.state.cubeName;
            if (cubeName) {
                var projName = null;
                angular.forEach($scope.projects, function (project, index) {
                    angular.forEach(project.datamodels, function (model, index) {
                        if (!projName && model.type=="CUBE"&&model.realization === cubeName) {
                            projName = project.name;
                        }
                    });
                });

                $scope.cubeMetaFrame.project = projName;
            }

            angular.forEach($scope.projects, function (project, index) {
                $scope.listAccess(project, 'ProjectInstance');
            });
        });
    }
});
