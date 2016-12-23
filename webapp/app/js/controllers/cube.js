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

KylinApp.controller('CubeCtrl', function ($scope, AccessService, MessageService, CubeService, TableService, ModelGraphService, UserService,SweetAlert,loadingRequest,modelsManager,$modal,cubesManager) {
    $scope.newAccess = null;
    $scope.state = {jsonEdit: false};

    $scope.modelsManager = modelsManager;
    $scope.cubesManager = cubesManager;

    $scope.buildGraph = function (cube) {
       ModelGraphService.buildTree(cube);
    };

    $scope.getCubeSql = function (cube) {
        CubeService.getSql({cubeId: cube.name, propValue: "null"}, function (sql) {
            cube.sql = sql.sql;
        },function(e){
            if(e.data&& e.data.exception){
                var message =e.data.exception;
                var msg = !!(message) ? message : 'Failed to take action.';
                SweetAlert.swal('Oops...', msg, 'error');
            }else{
                SweetAlert.swal('Oops...', "Failed to take action.", 'error');
            }
        });
    };

    $scope.getNotifyListString = function (cube) {
        if (cube.detail.notify_list) {
            cube.notifyListString = cube.detail.notify_list.join(",");
        }
        else {
            cube.notifyListString = "";
        }
    };

    $scope.cleanStatus = function(cube){

        if (!cube)
        {
            return;
        }
        var newCube = jQuery.extend(true, {}, cube);
        delete newCube.project;

        angular.forEach(newCube.dimensions, function(dimension, index){
            delete dimension.status;
        });

        return newCube;
    };

    $scope.updateNotifyList = function (cube) {
        cube.detail.notify_list = cube.notifyListString.split(",");
        CubeService.updateNotifyList({cubeId: cube.name}, cube.detail.notify_list, function () {
            SweetAlert.swal('Success!', 'Notify List updated successfully!', 'success');
        },function(e){
            if(e.data&& e.data.exception){
                var message =e.data.exception;
                var msg = !!(message) ? message : 'Failed to take action.';
                SweetAlert.swal('Oops...', msg, 'error');
            }else{
                SweetAlert.swal('Oops...', "Failed to take action.", 'error');
            }
        });
    };

    $scope.getHbaseInfo = function (cube) {
        if (!cube.hbase) {
            CubeService.getHbaseInfo({cubeId: cube.name, propValue: null, action: null}, function (hbase) {
                cube.hbase = hbase;

                TableService.get({tableName:cube.model.fact_table},function(table) {
                  if (table && table.source_type == 1) {
                    cube.streaming = true;
                  }
                })

                // Calculate cube total size based on each htable.
                var totalSize = 0;
                hbase.forEach(function(t) {
                    totalSize += t.tableSize;
                });
                cube.totalSize = totalSize;
            },function(e){
                if(e.data&& e.data.exception){
                    var message =e.data.exception;
                    var msg = !!(message) ? message : 'Failed to take action.';
                    SweetAlert.swal('Oops...', msg, 'error');
                }else{
                    SweetAlert.swal('Oops...', "Failed to take action.", 'error');
                }
            });
        }
    };

});

