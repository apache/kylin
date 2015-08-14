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

KylinApp
  .controller('SourceMetaCtrl', function ($scope, $cacheFactory, $q, $window, $routeParams, CubeService, $modal, TableService, $route, loadingRequest, SweetAlert, tableConfig, TableModel) {
    var $httpDefaultCache = $cacheFactory.get('$http');
    $scope.tableModel = TableModel;
    $scope.tableModel.selectedSrcDb = [];
    $scope.tableModel.selectedSrcTable = {};
    $scope.window = 0.68 * $window.innerHeight;
    $scope.tableConfig = tableConfig;


    $scope.state = {
      filterAttr: 'id', filterReverse: false, reverseColumn: 'id',
      dimensionFilter: '', measureFilter: ''
    };

    function innerSort(a, b) {
      var nameA = a.name.toLowerCase(), nameB = b.name.toLowerCase();
      if (nameA < nameB) //sort string ascending
        return -1;
      if (nameA > nameB)
        return 1;
      return 0; //default return value (no sorting)
    };

    $scope.aceSrcTbLoaded = function (forceLoad) {
      //stop request when project invalid
      if(!$scope.projectModel.getSelectedProject()){
        TableModel.init();
        return;
      }
      if (forceLoad) {
        $httpDefaultCache.removeAll();
      }
      $scope.loading = true;
      TableModel.aceSrcTbLoaded(forceLoad).then(function () {
        $scope.loading = false;
      },function(resp){
        SweetAlert.swal('Oops...',resp, 'error');
      });
    };
    $scope.aceSrcTbLoaded();
    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
//         will load table when enter this page,null or not
      if (newValue != oldValue || newValue == null) {
        $scope.aceSrcTbLoaded();
      }
    });


    $scope.showSelected = function (obj) {
      if (obj.uuid) {
        $scope.tableModel.selectedSrcTable = obj;
      }
      else if (obj.datatype) {
        $scope.tableModel.selectedSrcTable.selectedSrcColumn = obj;
      }
    };

    $scope.aceSrcTbChanged = function () {
      $scope.tableModel.selectedSrcDb = [];
      $scope.tableModel.selectedSrcTable = {};
      $scope.aceSrcTbLoaded(true);
    };


    $scope.openModal = function () {
      $modal.open({
        templateUrl: 'addHiveTable.html',
        controller: ModalInstanceCtrl,
        resolve: {
          tableNames: function () {
            return $scope.tableNames;
          },
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          scope: function () {
            return $scope;
          }
        }
      });
    };

    var ModalInstanceCtrl = function ($scope, $location, $modalInstance, tableNames, MessageService, projectName, scope) {
      $scope.tableNames = "";
      $scope.projectName = projectName;
      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };
      $scope.add = function () {
        if ($scope.tableNames.trim() === "") {
          SweetAlert.swal('', 'Please input table(s) you want to synchronize.', 'info');
          return;
        }

        if (!$scope.projectName) {
          SweetAlert.swal('', 'Please choose your project first!.', 'info');
          return;
        }

        $scope.cancel();
        loadingRequest.show();
        TableService.loadHiveTable({tableName: $scope.tableNames, action: projectName}, {}, function (result) {
          var loadTableInfo = "";
          angular.forEach(result['result.loaded'], function (table) {
            loadTableInfo += "\n" + table;
          })
          var unloadedTableInfo = "";
          angular.forEach(result['result.unloaded'], function (table) {
            unloadedTableInfo += "\n" + table;
          })

          if (result['result.unloaded'].length != 0 && result['result.loaded'].length == 0) {
            SweetAlert.swal('Failed!', 'Failed to synchronize following table(s): ' + unloadedTableInfo, 'error');
          }
          if (result['result.loaded'].length != 0 && result['result.unloaded'].length == 0) {
            SweetAlert.swal('Success!', 'The following table(s) have been successfully synchronized: ' + loadTableInfo, 'success');
          }
          if (result['result.loaded'].length != 0 && result['result.unloaded'].length != 0) {
            SweetAlert.swal('Partial loaded!', 'The following table(s) have been successfully synchronized: ' + loadTableInfo + "\n\n Failed to synchronize following table(s):" + unloadedTableInfo, 'warning');
          }
          loadingRequest.hide();
          scope.aceSrcTbLoaded(true);

        }, function (e) {
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Failed to take action.';
            SweetAlert.swal('Oops...', msg, 'error');
          } else {
            SweetAlert.swal('Oops...', "Failed to take action.", 'error');
          }
          loadingRequest.hide();
        })
      }
    };
    $scope.trimType = function (typeName) {
      if (typeName.match(/VARCHAR/i)) {
        typeName = "VARCHAR";
      }

      return typeName.trim().toLowerCase();
    }
  });

