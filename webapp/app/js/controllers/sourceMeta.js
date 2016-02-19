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
  .controller('SourceMetaCtrl', function ($scope, $cacheFactory, $q, $window, $routeParams, CubeService, $modal, TableService, $route, loadingRequest, SweetAlert, tableConfig, TableModel,cubeConfig) {
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
      if (!$scope.projectModel.getSelectedProject()) {
        TableModel.init();
        return;
      }
      if (forceLoad) {
        $httpDefaultCache.removeAll();
      }
      $scope.loading = true;
      TableModel.aceSrcTbLoaded(forceLoad).then(function () {
        $scope.loading = false;
      });
    };

    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
//         will load table when enter this page,null or not
      $scope.aceSrcTbLoaded();
    }, function (resp) {
      SweetAlert.swal('Oops...', resp, 'error');
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
        backdrop : 'static',
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


    //streaming model
    $scope.openStreamingSourceModal = function () {
      $modal.open({
        templateUrl: 'addStreamingSource.html',
        controller: StreamingSourceCtrl,
        backdrop : 'static',
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

    var StreamingSourceCtrl = function ($scope, $location, $modalInstance, tableNames, MessageService, projectName, scope, tableConfig,cubeConfig) {
      $scope.projectName = projectName;
      $scope.tableConfig = tableConfig;
      $scope.cubeConfig = cubeConfig;
      $scope.streaming = {
        sourceSchema: '',
        'parseResult': {}
      }

      $scope.table = {
        name: '',
        sourceValid:false
      }

      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };

      $scope.streamingOnLoad = function () {
        console.log($scope.streaming.sourceSchema);
      }

      $scope.columnList = [];

      $scope.streamingOnChange = function () {
        console.log($scope.streaming.sourceSchema);
        try {
          $scope.streaming.parseResult = JSON.parse($scope.streaming.sourceSchema);
        } catch (error) {
          $scope.table.sourceValid = false;
          console.log(error);
          return;
        }
        $scope.table.sourceValid = true;
        var columnList = [];
        for (var key in $scope.streaming.parseResult) {
          var defaultType="varchar(256)";
          var _value = $scope.streaming.parseResult[key];
          var defaultChecked = "Y";
          if(typeof _value ==="string"){
            defaultType="varchar(256)";
          }else if(typeof _value ==="number"){
            if(_value <= 2147483647){
              if(_value.toString().indexOf(".")!=-1){
                defaultType="decimal";
              }else{
                defaultType="int";
              }
            }else{
              defaultType="timestamp";
            }
          }
          if(defaultType=="timestamp"){
            defaultChecked = "N";
          }
          columnList.push({
            'name': key,
            'checked': defaultChecked,
            'type': defaultType,
            'fromSource':'Y'
          });



          //var formatList = [];
          //var
          columnList = _.sortBy(columnList, function (i) { return i.type; });
        }

          var timeMeasure = $scope.cubeConfig.streamingAutoGenerateMeasure;
          for(var i = 0;i<timeMeasure.length;i++){
            var defaultCheck = 'Y';
            columnList.push({
              'name': timeMeasure[i].name,
              'checked': defaultCheck,
              'type': timeMeasure[i].type,
              'fromSource':'N'
            });
          }

        if($scope.columnList.length==0){
          $scope.columnList = columnList;
        }

        angular.forEach(columnList,function(item){
          var included = false;
          for(var i=0;i<$scope.columnList.length;i++){
            if($scope.columnList[i].name==item.name){
              included = true;
              break;
            }
          }
          if(!included){
            $scope.columnList.push(item);
          }
        })

      }

      $scope.form={};
      $scope.rule={
        'timestampColumnConflict':false
      }
      $scope.syncStreamingSchema = function () {
        $scope.form['setStreamingSchema'].$sbumitted = true;
        if(!$scope.streaming.sourceSchema||$scope.streaming.sourceSchema===""){
          return;
        }

        if(!$scope.table.name||$scope.table.name===""){
          return;
        }

        var columns = [];
        angular.forEach($scope.columnList,function(column,$index){
          if (column.checked == "Y") {
            var columnInstance = {
              "id": ++$index,
              "name": column.name,
              "datatype": column.type
            }
            columns.push(columnInstance);
          }
        })


        $scope.tableData = {
          "name": $scope.table.name,
          "columns": columns,
          'database':'Default'
        }

        SweetAlert.swal({
          title: '',
          text: 'Are you sure to create the streaming table info?',
          type: '',
          showCancelButton: true,
          confirmButtonColor: '#DD6B55',
          confirmButtonText: "Yes",
          closeOnConfirm: true
        }, function (isConfirm) {
          if (isConfirm) {
            loadingRequest.show();
            TableService.addStreamingSrc({}, {project: $scope.projectName,tableData:angular.toJson($scope.tableData)}, function (request) {
              if(request.success){
                loadingRequest.hide();
                SweetAlert.swal('', 'Create Streaming Table Schema Successfully.', 'success');
                $scope.cancel();
                scope.aceSrcTbLoaded(true);
                return;
              }else{
                SweetAlert.swal('Oops...', "Failed to take action.", 'error');
              }
              //end loading
              loadingRequest.hide();
            }, function (e) {
              if (e.data && e.data.exception) {
                var message = e.data.exception;
                var msg = !!(message) ? message : 'Failed to take action.';
                SweetAlert.swal('Oops...', msg, 'error');
              } else {
                SweetAlert.swal('Oops...', "Failed to take action.", 'error');
              }
              loadingRequest.hide();
            });
          }
        });
      }
    };

  });

