/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

'use strict';

KylinApp.controller('CubeSchemaCtrl', function ($scope, QueryService, UserService, ProjectService, AuthenticationService,$filter,ModelService,MetaModel) {

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


    // ~ init
    if (!$scope.state) {
        $scope.state = {mode: "view"};
    }

    $scope.$watch('cube.detail', function (newValue, oldValue) {
        if(!newValue){
            return;
        }
        if (newValue&&$scope.state.mode==="view") {
            $scope.cubeMetaFrame = newValue;

            // when viw state,each cubeSchema has its own metaModel
            $scope.metaModel={
                model:{}
            }

            //init model
            ModelService.get({model_name: $scope.cubeMetaFrame.model_name}, function (model) {
                if (model) {
//                    $scope.metaModel = MetaModel;

                    $scope.metaModel.model = model;

                    //convert GMT mills ,to make sure partition date show GMT Date
                    //should run only one time
                    if($scope.metaModel.model.partition_desc&&$scope.metaModel.model.partition_desc.partition_date_start)
                    {
                        $scope.metaModel.model.partition_desc.partition_date_start+=new Date().getTimezoneOffset()*60000;
                    }
                }
            });

        }
    });

    $scope.$watch('cubeMetaFrame', function (newValue, oldValue) {
        if(!newValue){
            return;
        }
        if ($scope.cubeMode=="editExistCube"&&newValue && !newValue.project) {
            initProject();
        }

    });

    // ~ public methods
    $scope.filterProj = function(project){
        return $scope.userService.hasRole('ROLE_ADMIN') || $scope.hasPermission(project,$scope.permissions.ADMINISTRATION.mask);
    };

    $scope.addNewMeasure = function (measure) {
        $scope.newMeasure = (!!measure)? measure:CubeDescModel.createMeasure();
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
                    angular.forEach(project.realizations, function (unit, index) {
                        if (!projName && unit.type=="CUBE"&&unit.realization === cubeName) {
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
