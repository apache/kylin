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

KylinApp.controller('HybridInstanceCtrl', function (
  $scope, $q, $location, $modal,
  ProjectModel, hybridInstanceManager, SweetAlert, HybridInstanceService, loadingRequest, MessageBox
) {
  $scope.projectModel = ProjectModel;
  $scope.hybridInstanceManager = hybridInstanceManager;

  //trigger init with directive []
  $scope.list = function () {
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
    queryParam.project = $scope.projectModel.selectedProject;
    return hybridInstanceManager.list(queryParam).then(function (resp) {
      defer.resolve(resp);
      hybridInstanceManager.loading = false;
      return defer.promise;
    });
  };

  $scope.list();

  $scope.$watch('projectModel.selectedProject', function() {
    $scope.list();
  });

  var HybridDetailModalCtrl = function ($scope, $modalInstance, hybrid) {
    $scope.hybrid = hybrid;
    $scope.hybridModels = [{
      name: hybrid.model
    }];

    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    };
  };

  $scope.openHybridDetailModal = function (hybrid) {
    $modal.open({
      templateUrl: 'hybridDetail.html',
      controller: HybridDetailModalCtrl,
      resolve: {
        hybrid: function () {
          return hybrid;
        }
      },
      windowClass: 'hybrid-detail'
    });
  };

  $scope.editHybridInstance = function(hybridInstance){
    // check for empty project of header, break the operation.
    if (ProjectModel.selectedProject === null) {
      SweetAlert.swal('Oops...', 'Please select your project first.', 'warning');
      $location.path("/models");
      return;
    }
    
    $location.path("/hybrid/edit/" + hybridInstance.name);
  };

  $scope.dropHybridInstance = function (hybridInstance) {

    // check for empty project of header, break the operation.
    if (ProjectModel.selectedProject === null) {
      SweetAlert.swal('Oops...', 'Please select your project first.', 'warning');
      $location.path("/models");
      return;
    }

    SweetAlert.swal({
      title: '',
      text: 'Are you sure to drop this hybrid?',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {
        var schema = {
          hybrid: hybridInstance.name,
          model: hybridInstance.model,
          project: hybridInstance.project,
        };

        loadingRequest.show();
        HybridInstanceService.drop(schema, {}, function (result) {
          loadingRequest.hide();
          MessageBox.successNotify('Hybrid drop is done successfully');
          location.reload();
        }, function (e) {
          loadingRequest.hide();
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Failed to take action.';
            SweetAlert.swal('Oops...', msg, 'error');
          } else {
            SweetAlert.swal('Oops...', "Failed to take action.", 'error');
          }
        });
      }

    });
  };
});