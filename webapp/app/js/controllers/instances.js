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
  .controller('InstanceCtrl', function ($scope, InstanceService, $q, $routeParams, $interval, $modal, kylinConfig, instanceConfig, SweetAlert, $window) {
    $scope.loading = true;
    $scope.jobNodes = [];
    $scope.queryNodes = [];
    $scope.selectedLeaders = [];
    $scope.activeJobNodes = [];
    $scope.instanceConfig = instanceConfig;


    $scope.list = function () {
      $scope.jobNodes = [];
      $scope.queryNodes = [];
      $scope.selectedLeaders = [];
      $scope.activeJobNodes = [];

      InstanceService.getInstances({}, function (state) {
        $scope.loading = false;
        if (state.data && state.code === "000") {
          $scope.jobNodes = state.data.jobNodes;
          $scope.queryNodes = state.data.queryNodes;
          $scope.selectedLeaders = state.data.selectedLeaders;
          $scope.activeJobNodes = state.data.activeJobNodes;
        } else {
          var msg = !!(state.msg) ? state.msg : 'Failed to load instances.';
          SweetAlert.swal('Oops...', msg, 'error');
        }
      });
    }

    $scope.list();

    $scope.getJobNodeType = function (node) {
      if ($scope.selectedLeaders.indexOf(node) >= 0) {
        return "Job Node (Leader)";
      } else {
        return "Job Node";
      }
    }

    $scope.isJobNodeActive = function (node) {
      return $scope.activeJobNodes.indexOf(node) >= 0;
    }
  });
