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

KylinApp.controller('CubeSchemaCtrl', function ($scope, QueryService, UserService,modelsManager, ProjectService, AuthenticationService,$filter,ModelService,MetaModel,CubeDescModel,CubeList,TableModel,ProjectModel,ModelDescService,SweetAlert,cubesManager,StreamingService,CubeService) {
    $scope.modelsManager = modelsManager;
    $scope.cubesManager = cubesManager;
    $scope.projects = [];
    $scope.newDimension = null;
    $scope.newMeasure = null;
    $scope.forms={};
    $scope.wizardSteps = [
        {title: 'Cube Info', src: 'partials/cubeDesigner/info.html', isComplete: false,form:'cube_info_form'},
        {title: 'Dimensions', src: 'partials/cubeDesigner/dimensions.html', isComplete: false,form:'cube_dimension_form'},
        {title: 'Measures', src: 'partials/cubeDesigner/measures.html', isComplete: false,form:'cube_measure_form'},
        {title: 'Refresh Setting', src: 'partials/cubeDesigner/refresh_settings.html', isComplete: false,form:'refresh_setting_form'},

    ];
    if (UserService.hasRole("ROLE_ADMIN")) {
            $scope.wizardSteps.push({title: 'Advanced Setting', src: 'partials/cubeDesigner/advanced_settings.html', isComplete: false,form:'cube_setting_form'});
    }
    $scope.wizardSteps.push({title: 'Streaming', src: 'partials/cubeDesigner/streamingConfig.html', isComplete: false,form:'cube_streaming_form'});
    $scope.wizardSteps.push({title: 'Overview', src: 'partials/cubeDesigner/overview.html', isComplete: false,form:null});

    $scope.curStep = $scope.wizardSteps[0];

    $scope.allCubes = [];


  // ~ init
    if (!$scope.state) {
        $scope.state = {mode: "view"};
    }

  var queryParam = {offset: 0, limit: 65535};

  CubeService.list(queryParam, function (all_cubes) {
    if($scope.allCubes.length > 0){
      $scope.allCubes.splice(0,$scope.allCubes.length);
    }

    for (var i = 0; i < all_cubes.length; i++) {
      $scope.allCubes.push(all_cubes[i].name.toUpperCase());
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
      console.log($scope.cubeMode);
        if($scope.cubeMode == "addNewCube"){
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

  $scope.$watch('cube.detail', function (newValue, oldValue) {
    if (!newValue) {
      return;
    }
    if (newValue && $scope.state.mode === "view") {
      $scope.cubeMetaFrame = newValue;

      // when viw state,each cubeSchema has its own metaModel
      $scope.metaModel = {
        model: {}
      }
      $scope.metaModel.model=modelsManager.getModel($scope.cubeMetaFrame.model_name);

      StreamingService.getConfig({cubeName:$scope.cubeMetaFrame.name}, function (kfkConfigs) {
        if(!!kfkConfigs[0]&&kfkConfigs[0].cubeName == $scope.cubeMetaFrame.name){
          $scope.cubeState.isStreaming = true;
          $scope.streamingMeta = kfkConfigs[0];
          StreamingService.getKfkConfig({kafkaConfigName:$scope.streamingMeta.name}, function (streamings) {
            $scope.kafkaMeta = streamings[0];
          })
        }
      })

    }
  });

  $scope.$watch('cubeMetaFrame', function (newValue, oldValue) {
    if (!newValue) {
      return;
    }
    if ($scope.cubeMode == "editExistCube" && newValue && !newValue.project) {
      initProject();
    }

  });

    $scope.checkCubeForm = function(stepIndex){
      // do not check for Prev Step
      if (stepIndex + 1 < $scope.curStep.step) {
        return true;
      }

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
                    case 'cube_info_form':
                      return $scope.check_cube_info();
                      break;
                    case 'cube_dimension_form':
                        return $scope.check_cube_dimension();
                        break;
                    case 'cube_measure_form':
                        return $scope.check_cube_measure();
                        break;
                    case 'cube_setting_form':
                        return $scope.check_cube_setting();
                    case 'cube_streaming_form':
                        return $scope.kafka_ad_config_form();
                    default:
                        return true;
                        break;
                }
            }
        }
    };

  $scope.check_cube_info = function(){
    if(($scope.state.mode === "edit") &&$scope.cubeMode=="addNewCube"&&($scope.allCubes.indexOf($scope.cubeMetaFrame.name.toUpperCase()) >= 0)){
      SweetAlert.swal('Oops...', "The cube named [" + $scope.cubeMetaFrame.name.toUpperCase() + "] already exists", 'warning');
      return false;
    }
    return true;
  }

    $scope.check_cube_dimension = function(){
        var errors = [];
        if(!$scope.cubeMetaFrame.dimensions.length){
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
        var _measures = $scope.cubeMetaFrame.measures;
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

        angular.forEach($scope.cubeMetaFrame.rowkey.aggregation_groups,function(group){
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

  $scope.kafka_ad_config_form = function(){
    if(!$scope.cubeState.isStreaming){
      return true;
    }
    var errors = [];
    if(!$scope.kafkaMeta.clusters.length){
      errors.push("Cluster can't be null");
    }
    angular.forEach($scope.kafkaMeta.clusters,function(cluster,index){
      if(!cluster.brokers.length){
        errors.push("No broker under Cluster-"+(index+1));
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
        ProjectService.listReadable({}, function (projects) {
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

                $scope.cubeMetaFrame.project = projName;
            }

            angular.forEach($scope.projects, function (project, index) {
                $scope.listAccess(project, 'ProjectInstance');
            });
        });
    }
});
