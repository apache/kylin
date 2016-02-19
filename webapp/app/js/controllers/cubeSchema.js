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

KylinApp.controller('CubeSchemaCtrl', function ($scope, QueryService, UserService, ProjectService, AuthenticationService, $filter, ModelService,CubeService, MetaModel, CubeDescModel, CubeList, TableModel, ProjectModel, SweetAlert) {

  $scope.projects = [];
  $scope.newDimension = null;
  $scope.newMeasure = null;
  $scope.allCubes = [];

  $scope.wizardSteps = [
    {title: 'Cube Info', src: 'partials/cubeDesigner/info.html', isComplete: false},
    {title: 'Data Model', src: 'partials/cubeDesigner/data_model.html', isComplete: false},
    {title: 'Dimensions', src: 'partials/cubeDesigner/dimensions.html', isComplete: false},
    {title: 'Measures', src: 'partials/cubeDesigner/measures.html', isComplete: false},
    {title: 'Filter', src: 'partials/cubeDesigner/filter.html', isComplete: false},
    {title: 'Refresh Setting', src: 'partials/cubeDesigner/incremental.html', isComplete: false}
  ];
  if (UserService.hasRole("ROLE_ADMIN")) {
    $scope.wizardSteps.push({
      title: 'Advanced Setting',
      src: 'partials/cubeDesigner/advanced_settings.html',
      isComplete: false
    });
  }
  $scope.wizardSteps.push({title: 'Overview', src: 'partials/cubeDesigner/overview.html', isComplete: false});

  $scope.curStep = $scope.wizardSteps[0];


  // ~ init
  if (!$scope.state) {
    $scope.state = {mode: "view"};
  }

  // Add all cube names to avoid name conflict during cube creation.
  var queryParam = {offset: 0, limit: 65535};

  CubeService.list(queryParam, function (all_cubes) {
      if($scope.allCubes.length > 0){
          $scope.allCubes.splice(0,$scope.allCubes.length);
      }

      for (var i = 0; i < all_cubes.length; i++) {
          $scope.allCubes.push(all_cubes[i].name.toUpperCase());
      }
  });

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

      //init model
      ModelService.get({model_name: $scope.cubeMetaFrame.model_name}, function (model) {
        if (model) {
          $scope.metaModel.model = model;
        }
      },function(resp){
        SweetAlert.swal('Oops...', "Failed to load model info, please check system log for details.", 'error');
      });
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

  // ~ public methods
  $scope.filterProj = function (project) {
    return $scope.userService.hasRole('ROLE_ADMIN') || $scope.hasPermission(project, $scope.permissions.ADMINISTRATION.mask);
  };

  $scope.addNewMeasure = function (measure) {
    $scope.newMeasure = (!!measure) ? measure : CubeDescModel.createMeasure();
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

  //map right return type for param
  $scope.measureReturnTypeUpdate = function () {

    if($scope.newMeasure.function.expression == 'COUNT'){
      $scope.newMeasure.function.parameter.type= 'constant';
    }

    if ($scope.newMeasure.function.parameter.type == "constant" && $scope.newMeasure.function.expression !== "COUNT_DISTINCT") {
      switch ($scope.newMeasure.function.expression) {
        case "SUM":
        case "COUNT":
          $scope.newMeasure.function.returntype = "bigint";
          break;
        default:
          $scope.newMeasure.function.returntype = "";
          break;
      }
    }

    if ($scope.newMeasure.function.parameter.type == "column" && $scope.newMeasure.function.expression !== "COUNT_DISTINCT") {

      var column = $scope.newMeasure.function.parameter.value;
      var colType = $scope.getColumnType(column, $scope.metaModel.model.fact_table); // $scope.getColumnType defined in cubeEdit.js

      if (colType == "" || !colType) {
        $scope.newMeasure.function.returntype = "";
        return;
      }


      switch ($scope.newMeasure.function.expression) {
        case "SUM":
          if (colType === "smallint" || colType === "int" || colType === "bigint"||colType==="integer") {
            $scope.newMeasure.function.returntype = 'bigint';
          } else {
            if (colType.indexOf('decimal') != -1) {
              $scope.newMeasure.function.returntype = colType;
            } else {
              $scope.newMeasure.function.returntype = 'decimal';
            }
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

  $scope.refreshAggregationGroup = function (list, index, aggregation_group) {
    if (aggregation_group) {
      list[index].length = aggregation_group.length;
      for(var i=0;i<aggregation_group.length;i++){
        list[index][i] = aggregation_group[i];
      }
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
    if(stepIndex === 0 && $scope.cubeMode=="addNewCube" && ($scope.allCubes.indexOf($scope.cubeMetaFrame.name.toUpperCase()) >= 0)){
        SweetAlert.swal('Oops...', "The cube named " + $scope.cubeMetaFrame.name.toUpperCase() + " already exists", 'error');
        return;
    }

    if (stepIndex < ($scope.wizardSteps.length - 1)) {
      $scope.curStep.isComplete = true;
      $scope.curStep = $scope.wizardSteps[stepIndex + 1];

      AuthenticationService.ping(function (data) {
        UserService.setCurUser(data);
      });
    }
  };

  $scope.goToStep = function (stepIndex) {
    for (var i = 0; i < $scope.wizardSteps.length; i++) {
      if (i <= stepIndex) {
        $scope.wizardSteps[i].isComplete = true;
      } else {
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

  // ~ private methods
  function initProject() {
    ProjectService.list({}, function (projects) {
      $scope.projects = projects;

      var cubeName = (!!$scope.routeParams.cubeName) ? $scope.routeParams.cubeName : $scope.state.cubeName;
      if (cubeName) {
        var projName = null;
        if (ProjectModel.getSelectedProject()) {
          projName = ProjectModel.getSelectedProject();
        } else {
          angular.forEach($scope.projects, function (project, index) {
            angular.forEach(project.realizations, function (unit, index) {
              if (!projName && unit.type == "CUBE" && unit.realization === cubeName) {
                projName = project.name;
              }
            });
          });
        }

        if (!ProjectModel.getSelectedProject()) {
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
