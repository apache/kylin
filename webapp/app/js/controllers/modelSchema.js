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

KylinApp.controller('ModelSchemaCtrl', function ($scope, QueryService, UserService, ProjectService, $q, AuthenticationService, $filter, ModelService, MetaModel, CubeDescModel, CubeList, TableModel, ProjectModel, $log, SweetAlert, modelsManager) {

  $scope.modelsManager = modelsManager;
  $scope.projectModel = ProjectModel;
  $scope.projects = [];
  $scope.newDimension = null;
  $scope.newMeasure = null;

  $scope.forms = {};

  // ~ init
  if (!$scope.state) {
    $scope.state = {mode: "view"};
  }


  $scope.wizardSteps = [
    {title: 'Model Info', src: 'partials/modelDesigner/model_info.html', isComplete: false, form: 'model_info_form'},
    {title: 'Data Model', src: 'partials/modelDesigner/data_model.html', isComplete: false, form: 'data_model_form'},
    {
      title: 'Dimensions',
      src: 'partials/modelDesigner/model_dimensions.html',
      isComplete: false,
      form: 'model_dimensions_form'
    },
    {
      title: 'Measures',
      src: 'partials/modelDesigner/model_measures.html',
      isComplete: false,
      form: 'model_measure_form'
    },
    {
      title: 'Settings',
      src: 'partials/modelDesigner/conditions_settings.html',
      isComplete: false,
      form: 'model_setting_form'
    }
  ];

  $scope.curStep = $scope.wizardSteps[0];

  //init modelsManager
  if ($scope.state.mode == "edit") {
    var defer = $q.defer();
    var queryParam = {};
    if (!$scope.projectModel.isSelectedProjectValid()) {
      return;
    }
    queryParam.projectName = $scope.projectModel.selectedProject;
    modelsManager.list(queryParam).then(function (resp) {
      defer.resolve(resp);
      modelsManager.loading = false;
      return defer.promise;
    });
  }

  $scope.$watch('model', function (newValue, oldValue) {
    if (!newValue) {
      return;
    }
    if ($scope.modelMode == "editExistModel" && newValue && !newValue.project) {
      initProject();
    }

  });

  // ~ public methods
  $scope.filterProj = function (project) {
    return $scope.userService.hasRole('ROLE_ADMIN') || $scope.hasPermission(project, $scope.permissions.ADMINISTRATION.mask);
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

  $scope.checkForm = function (stepIndex) {
    // do not check for Prev Step
    if (stepIndex + 1 < $scope.curStep.step) {
      return true;
    }

    if (!$scope.curStep.form) {
      return true;
    }
    if ($scope.state.mode === 'view') {
      return true;
    }
    else {
      //form validation
      if ($scope.forms[$scope.curStep.form].$invalid) {
        $scope.forms[$scope.curStep.form].$submitted = true;
        return false;
      } else {
        //business rule check
        switch ($scope.curStep.form) {
          case 'data_model_form':
            return $scope.check_data_model();
            break;
          case 'model_info_form':
            return $scope.check_model_info();
            break;
          case 'model_dimensions_form':
            return $scope.check_model_dimensions();
            break;
          case 'model_measure_form':
            return $scope.check_model_measure();
            break;
          case 'model_setting_form':
            return $scope.check_model_setting();
            break;
          default:
            return true;
            break;
        }
      }
    }
  };

  $scope.check_model_info = function () {

    var modelName = $scope.modelsManager.selectedModel.name.toUpperCase();
    var models = $scope.modelsManager.modelNameList;
    if ($scope.modelMode=="addNewModel") {
      if(models.indexOf(modelName) != -1 || models.indexOf(modelName.toLowerCase()) !=-1){
        SweetAlert.swal('', "Model named [" + modelName + "] already exist!", 'warning');
        return false;
      }
    }
    return true;
  }

  /*
   * lookups can't be null
   */
  $scope.check_data_model = function () {
    var errors = [];
//        if(!modelsManager.selectedModel.lookups.length){
//            errors.push("No lookup table defined");
//        }
    var errorInfo = "";
    angular.forEach(errors, function (item) {
      errorInfo += "\n" + item;
    });
    if (errors.length) {
      SweetAlert.swal('', errorInfo, 'warning');
      return false;
    } else {
      return true;
    }

  }

  /*
   * dimensions validation
   * 1.dimension can't be null
   */
  $scope.check_model_dimensions = function () {

    var errors = [];
    if (!modelsManager.selectedModel.dimensions.length) {
      errors.push("No dimensions defined.");
    }
    var isDimensionDefined = false;
    angular.forEach(modelsManager.selectedModel.dimensions, function (_dimension) {
      if(_dimension.columns && _dimension.columns.length){
        isDimensionDefined = true;
      }
    });
    if(!isDimensionDefined){
      errors.push("No dimensions defined.");
    }
    var errorInfo = "";
    angular.forEach(errors, function (item) {
      errorInfo += "\n" + item;
    });
    if (errors.length) {
      SweetAlert.swal('Oops...', errorInfo, 'warning');
      return false;
    } else {
      return true;
    }

  };

  /*
   * dimensions validation
   * 1.metric can't be null
   */
  $scope.check_model_measure = function () {
    return true;
  };
  $scope.check_model_setting = function () {
    return true;
  }


  $scope.goToStep = function (stepIndex) {
    if ($scope.modelMode == "addNewModel") {
      if (stepIndex + 1 >= $scope.curStep.step) {
        return;
      }
    }
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

  }
});
