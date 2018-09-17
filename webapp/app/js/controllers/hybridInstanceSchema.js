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

KylinApp.controller('HybridInstanceSchema', function (
  $scope, $q, $location, $interpolate, $templateCache, $routeParams,
  CubeList, HybridInstanceService, ProjectModel, modelsManager, SweetAlert, MessageService, loadingRequest, CubeService, CubeDescService, MessageBox
) {

  // check for empty project of header, break the operation.
  if (!$scope.isEdit && ProjectModel.selectedProject === null) {
    SweetAlert.swal('Oops...', 'Please select your project first.', 'warning');
    $location.path("/models");
    return;
  }

  $scope.LEFT = 'LEFT';
  $scope.RIGHT = 'RIGHT';
  $scope.isFormDisabled = false;

  $scope.cubeList = CubeList;
  $scope.projectModel = ProjectModel;
  $scope.modelsManager = modelsManager;

  $scope.route = { params: $routeParams.hybridName };
  $scope.isEdit = !!$routeParams.hybridName;

  $scope.isEditInitialized = false;
  $scope.isLockEditModel = false;

  $scope.form = {
    name: '',
    model: ''
  };
  $scope.$validator = {}

  resetPageData();

  /**
   * Computed: get the model's cubes
   * 
   * @param {'LEFT' | 'RIGHT'} dir 
   */
  $scope.getFiltedModelCube = function(dir) {
    var dataRows = $scope.table[dir].dataRows;

    return dataRows.filter(function(row) {
      return row.model === $scope.form.model;
    });
  }

  /**
   * Computed: get the count of the model cubes
   * 
   * @param {'LEFT' | 'RIGHT'} dir 
   */
  $scope.getFiltedModelCubeCount = function(dir) {
    return $scope.getFiltedModelCube(dir).length;
  }

  /**
   * Computed: judge that current cube row is checked
   * 
   * @param {'LEFT' | 'RIGHT'} dir 
   * @param {*} cube 
   */
  $scope.isCubeChecked = function(dir, cube) {
    return $scope.table[dir].checkedCubeIds.indexOf(function(cubeId) {
      return cubeId === cube.uuid;
    }) !== -1;
  };

  /**
   * Computed: judge that all rows of the table are checked
   * 
   * @param {'LEFT' | 'RIGHT'} dir 
   */
  $scope.isCheckAll = function(dir) {
    var dataRows = $scope.getFiltedModelCube(dir);

    return dataRows.length ? dataRows.every(function(row) {
      return row.isChecked === true;
    }) : false;
  };

  $scope.toggleCube = function(cube) {
    cube.isChecked = !cube.isChecked;
  }

  /**
   * Computed: judge that model select component can be chosen
   */
  $scope.isModelSelectDisabled = function() {
    return !modelsManager.models.length
      || $scope.table[$scope.RIGHT].dataRows.length;
  }

  /**
   * Computed: judge is form valid
   */
  $scope.isFormValid = function() {
    // get schema data
    var schema = getSchema();

    return Object.keys(schema).every(function(key) {
      // otherwise checking empty value
      return !!schema[key];
    });
  };

  /**
   * Action: toggle all rows' check status of the table
   * 
   * @param {'LEFT' | 'RIGHT'} dir 
   * @param {undefined | boolean} toStatus: for change all the table's cubes hardly
   */
  $scope.toggleAll = function(dir, toStatus) {
    var isCheckAll = $scope.isCheckAll(dir);
    var dataRows = $scope.getFiltedModelCube(dir);

    dataRows.forEach(function(row) {
      if(toStatus !== undefined) {
        row.isChecked = toStatus;
      } else {
        row.isChecked = !isCheckAll;
      }
    });
  };

  /**
   * Action: transfer checked rows from destination table to source table
   * 
   * @param {'LEFT' | 'RIGHT'} dir 
   */
  $scope.transferTo = function(dir) {
    var toDir = dir;
    var fromDir = dir === $scope.RIGHT ? $scope.LEFT : $scope.RIGHT;
    var srcTable = $scope.table[fromDir];
    var disTable = $scope.table[toDir];

    // get checked rows from source table to transfer rows
    var transferRows = srcTable.dataRows.filter(function(row) {
      return row.isChecked;
    });

    // filter unchecked row to source table rows
    srcTable.dataRows = srcTable.dataRows.filter(function(row) {
      return !row.isChecked;
    });

    // clean transfer rows check status
    transferRows.forEach(function(row) {
      row.isChecked = false;
    });

    // push transfer rows to destination table
    disTable.dataRows = disTable.dataRows.concat(transferRows);
  }

  /**
   * Action: page edit cancel handler
   */
  $scope.cancel = function() {
    history.go(-1);
  };

  /**
   * Action: page edit submit handler
   */
  $scope.submit = function() {
    // get form data
    var schema = getSchema();
    // show save warning
    saveWarning(function() {
      // show loading
      loadingRequest.show();
      // save the hybrid cube
      if(!$scope.isEdit) {
        HybridInstanceService.save({}, schema, successHandler, failedHandler);
      } else {
        HybridInstanceService.update({}, schema, successHandler, failedHandler);
      }
    });

    function successHandler(request) {
      if(request.successful === false) {
        var message = request.message;
        var msg = !!message ? message : 'Failed to take action.';
        var template = hybridInstanceResultTmpl({ text: msg, schema: schema });
        MessageService.sendMsg(template, 'error', {}, true, 'top_center');
      } else {
        if($scope.isEdit) {
          MessageBox.successNotify('Update hybrid cube successfully.');
        } else {
          MessageBox.successNotify('Create hybrid cube successfully.');
        }
        $location.path('/models');
      }
      // hide global loading
      loadingRequest.hide();
    }

    function failedHandler(e) {
      if (e.data && e.data.exception) {
        var message = e.data.exception;
        var msg = !!(message) ? message : 'Failed to take action.';
        var template = hybridInstanceResultTmpl({ text: msg, schema: schema });
        MessageService.sendMsg(template, 'error', {}, true, 'top_center');
      } else {
        var template = hybridInstanceResultTmpl({ text: 'Failed to take action.', schema: schema });
        MessageService.sendMsg(template, 'error', {}, true, 'top_center');
      }
      // hide global loading
      loadingRequest.hide();
    }
  }

  doPerpare();

  /**
   * Init: initialize watcher
   */
  function doPerpare() {
    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
      if (newValue != oldValue || newValue == null) {
        CubeList.removeAll();
        resetPageData();
        listModels();
      }
    });

    $scope.$watch('modelsManager.models', function() {
      $scope.form.model = modelsManager.models[0] && modelsManager.models[0].name || '';
    });

    $scope.$watch('form.model', function() {
      if(!$scope.isLockEditModel) {
        cleanCubeStatus();
      }
      $scope.isLockEditModel = false;
    });

    $scope.$watch('cubeList.cubes', function() {
      loadTableData();

      if ($scope.isEdit && !$scope.isEditInitialized && CubeList.cubes.length) {
        getEditHybridInstance();
        $scope.isEditInitialized = true;
      }
    });
  }

  /**
   * Helper: get form data
   */
  function getSchema() {
    const schema = {
      hybrid: $scope.form.name,
      project: $scope.projectModel.selectedProject,
      model: $scope.form.model,
      cubes: $scope.table[$scope.RIGHT].dataRows.map(function(row) {
        return row.name;
      })
    };
    return schema;
  }

  /**
   * Helper: reset page data
   */
  function resetPageData() {
    $scope.table = {};
    $scope.form.model = '';
    $scope.table[$scope.LEFT] = {
      dataRows: []
    };
    $scope.table[$scope.RIGHT] = {
      dataRows: []
    };
  }

  /**
   * Helper: ajax request models
   */
  function listModels () {
    var defer = $q.defer();
    var queryParam = {};
    if (!$scope.projectModel.isSelectedProjectValid()) {
      defer.resolve([]);
      return defer.promise;
    }

    if (!$scope.projectModel.projects.length) {
      defer.resolve([]);
      return defer.promise;
    }
    queryParam.projectName = $scope.projectModel.selectedProject;
    return modelsManager.list(queryParam).then(function (resp) {
      defer.resolve(resp);
      modelsManager.loading = false;
      return defer.promise;
    });
  };

  /**
   * Helper: clean left table and reset status
   */
  function loadTableData() {
    var cubesData = Object.create($scope.cubeList.cubes);
    var unusedCubeTable = $scope.table[$scope.LEFT].dataRows = [];

    cubesData.forEach(function(cubeData) {
      cubeData.isChecked = false;
      unusedCubeTable.push(cubeData);
    });
  }

  function hybridInstanceResultTmpl(notification) {
    // Get the static notification template.
    var tmpl = notification.type == 'success' ? 'hybridResultSuccess.html' : 'hybridResultError.html';
    return $interpolate($templateCache.get(tmpl))(notification);
  };

  function saveWarning(callback) {
    SweetAlert.swal({
      title: $scope.isEdit
        ? 'Are you sure to update the Hybrid Cube?'
        : 'Are you sure to save the Hybrid Cube?',
      text: $scope.isEdit
        ? ''
        : '',
      type: 'warning',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
  }, function(isConfirm) {
    if(isConfirm) {
      callback();
    }
  })};

  /**
   * Helper: if $scope.form.model is changed, clean all the selected cube.
   */
  function cleanCubeStatus() {
    // clean left table cubes
    $scope.table[$scope.LEFT].dataRows.forEach(function(row) {
      row.isChecked = false;
    });
    // check right table cubes
    $scope.table[$scope.RIGHT].dataRows.forEach(function(row) {
      row.isChecked = true;
    });
    // move right table cubes to left table
    $scope.transferTo($scope.LEFT);
  }

  /**
   * Helper: get edit hybrid cube
   */
  function getEditHybridInstance() {
    loadingRequest.show();

    HybridInstanceService.getByName({ hybrid_name: $routeParams.hybridName }, function (_hybridInstance) {
      var hybridInstance = _hybridInstance.hybridInstance;

      $scope.form.uuid = hybridInstance.uuid;
      $scope.form.name = hybridInstance.name;

      hybridInstance.realizations.forEach(function(realizationItem) {
        var usedCubeName = realizationItem.realization;
        var unusedCubeTable = $scope.table[$scope.LEFT];

        unusedCubeTable.dataRows.forEach(function(row) {
          if(row.name === usedCubeName)  {
            row.isChecked = true;
            $scope.form.model = row.model;
          }
        });
      });

      $scope.transferTo($scope.RIGHT);
      loadingRequest.hide();
      $scope.isLockEditModel = true;
    });
  }
});