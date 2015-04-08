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

KylinApp.controller('CubeSchemaCtrl', function ($scope, QueryService, UserService, ProjectService, AuthenticationService,$filter,ModelService,MetaModel,CubeDescModel,CubeList,TableModel,ProjectModel,ModelDescService,SweetAlert,cubesManager) {

    $scope.cubesManager = cubesManager;
    $scope.projects = [];
    $scope.newDimension = null;
    $scope.newMeasure = null;
    $scope.forms={};
    $scope.wizardSteps = [
        {title: 'Cube Info', src: 'partials/cubeDesigner/info.html', isComplete: false,form:'cube_info_form'},
        {title: 'Dimensions', src: 'partials/cubeDesigner/dimensions.html', isComplete: false,form:'cube_dimension_form'},
        {title: 'Measures', src: 'partials/cubeDesigner/measures.html', isComplete: false,form:'cube_measure_form'},
    ];
    if (UserService.hasRole("ROLE_ADMIN")) {
            $scope.wizardSteps.push({title: 'Advanced Setting', src: 'partials/cubeDesigner/advanced_settings.html', isComplete: false,form:'cube_setting_form'});
    }
    $scope.wizardSteps.push({title: 'Overview', src: 'partials/cubeDesigner/overview.html', isComplete: false,form:null});

    $scope.curStep = $scope.wizardSteps[0];


    // ~ init
    if (!$scope.state) {
        $scope.state = {mode: "view"};
    }

    $scope.$watch('cubesManager.cubeMetaFrame', function (newValue, oldValue) {
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
        if (cubesManager.cubeMetaFrame.measures.indexOf($scope.newMeasure) === -1) {
            cubesManager.cubeMetaFrame.measures.push($scope.newMeasure);
        }
        $scope.newMeasure = null;
    };

    //map right return type for param
    $scope.measureReturnTypeUpdate = function(){
        if($scope.newMeasure.function.expression!=="COUNT_DISTINCT"){

            var column = $scope.newMeasure.function.parameter.value;
            var colType = $scope.getColumnType(column, $scope.metaModel.model.fact_table); // $scope.getColumnType defined in cubeEdit.js


            switch($scope.newMeasure.function.expression){
                case "SUM":
                    if(colType==="smallint"||colType==="int"||colType==="bigint"){
                        $scope.newMeasure.function.returntype= 'bigint';
                    }else{
                        $scope.newMeasure.function.returntype= 'decimal';
                    }
                    break;
                case "MIN":
                case "MAX":
                    $scope.newMeasure.function.returntype = colType;
                    break;
                case "COUNT":
                    $scope.newMeasure.function.returntype = "bigint";
                    break;
                default:
                    $scope.newMeasure.function.returntype = "";
                    break;
            }
        }
    }

    $scope.addNewRowkeyColumn = function () {
        cubesManager.cubeMetaFrame.rowkey.rowkey_columns.push({
            "column": "",
            "length": 0,
            "dictionary": "true",
            "mandatory": false
        });
    };

    $scope.addNewAggregationGroup = function () {
        cubesManager.cubeMetaFrame.rowkey.aggregation_groups.push([]);
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

    $scope.goToStep = function(stepIndex){
        if($scope.state.mode=="edit"){
            if(stepIndex+1>=$scope.curStep.step){
                return;
            }
        }
        for(var i=0;i<$scope.wizardSteps.length;i++){
            if(i<=stepIndex){
                $scope.wizardSteps[i].isComplete = true;
            }else{
                $scope.wizardSteps[i].isComplete = false;
            }
        }
        if (stepIndex < ($scope.wizardSteps.length)) {
            $scope.curStep = $scope.wizardSteps[stepIndex];

            AuthenticationService.ping(function (data) {
                UserService.setCurUser(data);
            });
        }
    }

    $scope.checkCubeForm = function(){
        if(!$scope.curStep.form){
            return true;
        }
        if($scope.state.mode==='view'){
            return true;
        }
        else{
            //form validation
            if($scope.forms[$scope.curStep.form].$invalid){
                $scope.forms[$scope.curStep.form].$submitted = true;
                return false;
            }else{
                //business rule check
                switch($scope.curStep.form){
                    case 'cube_dimension_form':
                        return $scope.check_cube_dimension();
                        break;
                    case 'cube_measure_form':
                        return $scope.check_cube_measure();
                        break;
                    case 'cube_setting_form':
                        return $scope.check_cube_setting();
                    default:
                        return true;
                        break;
                }
            }
        }
    };


    $scope.check_cube_dimension = function(){
        var errors = [];
        if(!cubesManager.cubeMetaFrame.dimensions.length){
            errors.push("Dimension can't be null");
        }
        var errorInfo = "";
        angular.forEach(errors,function(item){
            errorInfo+="\n"+item;
        });
        if(errors.length){
            SweetAlert.swal('', errorInfo, 'warning');
            return false;
        }else{
            return true;
        }
    };

    $scope.check_cube_measure = function(){
        var _measures = cubesManager.cubeMetaFrame.measures;
        var errors = [];
        if(!_measures||!_measures.length){
            errors.push("Please define your metrics.");
        }

        var existCountExpression = false;
        for(var i=0;i<_measures.length;i++){
            if(_measures[i].function.expression=="COUNT"){
                existCountExpression=true;
                break;
            }
        }
        if(!existCountExpression){
            errors.push("[COUNT] metric is required.");
        }

        var errorInfo = "";
        angular.forEach(errors,function(item){
            errorInfo+="\n"+item;
        });
        if(errors.length){
            SweetAlert.swal('', errorInfo, 'warning');
            return false;
        }else{
            return true;
        }
    }

    $scope.check_cube_setting = function(){
        var errors = [];

        angular.forEach(cubesManager.cubeMetaFrame.rowkey.aggregation_groups,function(group){
            if(!group.length){
                errors.push("Each aggregation group can't be empty.");
            }
        })

        var errorInfo = "";
        angular.forEach(errors,function(item){
            errorInfo+="\n"+item;
        });
        if(errors.length){
            SweetAlert.swal('', errorInfo, 'warning');
            return false;
        }else{
            return true;
        }
    }


    // ~ private methods
    function initProject() {
        ProjectService.list({}, function (projects) {
            $scope.projects = projects;

            var cubeName = (!!$scope.routeParams.cubeName)? $scope.routeParams.cubeName:$scope.state.cubeName;
            if (cubeName) {
                var projName = null;
                if(ProjectModel.getSelectedProject()){
                    projName=ProjectModel.getSelectedProject();
                }else{
                    angular.forEach($scope.projects, function (project, index) {
                        angular.forEach(project.realizations, function (unit, index) {
                            if (!projName && unit.type=="CUBE"&&unit.realization === cubeName) {
                                projName = project.name;
                            }
                        });
                    });
                }

                if(!ProjectModel.getSelectedProject()){
                    ProjectModel.setSelectedProject(projName);
                    TableModel.aceSrcTbLoaded();
                }

                cubesManager.cubeMetaFrame.project = projName;
            }

            angular.forEach($scope.projects, function (project, index) {
                $scope.listAccess(project, 'ProjectInstance');
            });
        });
    }
});
