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

KylinApp.controller('BalanceReplicaSetCtrl', function ($scope, $modal, AdminStreamingService, MessageService, AuthenticationService) {

  AdminStreamingService.getBalanceRecommend({}, function(data) {
    $scope.replicaSets = data;
  }, function(e) {
    if (e.data && e.data.exception) {
      var message = e.data.exception;
      var msg = !!(message) ? message : 'Failed get balance plan';
      SweetAlert.swal('Oops...', msg, 'error');
    } else {
      SweetAlert.swal('Oops...', 'Failed get balance plan', 'error');
    }
  });

  $scope.rebalance = function() {
    AdminStreamingService.reBalance({}, {reBalancePlan: $scope.replicaSets}, function(data) {
      SweetAlert.swal('Success!', 'Rebalance Success', 'success');
    }, function(e) {
      if (e.data && e.data.exception) {
        var message = e.data.exception;
        var msg = !!(message) ? message : 'Failed to rebalance';
        SweetAlert.swal('Oops...', msg, 'error');
      } else {
        SweetAlert.swal('Oops...', 'Failed to rebalance', 'error');
      }
    });
  };

  $scope.editReplicaSet = function(replicaSet, index) {
    var modalInstance = $modal.open({
      templateUrl: 'editReplicaSet.html',
      controller: function($scope, replicaSet, $modalInstance, AdminStreamingService) {
        $scope.replicaSet = angular.copy(replicaSet);

        $scope.saveReplicaSet =function() {
          console.log($scope.replicaSet);
          $modalInstance.close($scope.replicaSet);
        };

        $scope.removeCubeInfo = function(index) {
          console.log('remove element:', $scope.replicaSet.cubeInfos[index]);
          $scope.replicaSet.cubeInfos.splice(index, 1); 
        };

        $scope.addCubeInfo = function() {
          $scope.replicaSet.cubeInfos.push({cubeName:"", partitions:[]});
        }

        $scope.cancel = function() {
          $modalInstance.dismiss('cancel');
        };
      },
      resolve: {
        replicaSet: function() {
          return replicaSet;
        }
      }
    });

    modalInstance.result.then(function (replicaSet) {
      $scope.replicaSets[index] = replicaSet;
    });
  };
});