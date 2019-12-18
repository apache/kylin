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

KylinApp.controller('UserProfileCtrl', function ($scope, $modal, AuthenticationService, UserService, ResponseUtil, kylinConfig) {
  $scope.currentUser = {};

  // for password changing
  var changePwdCtrl = function ($scope, $modalInstance, SweetAlert) {
    $scope.userPattern = /^[\w.@]+$/;
    $scope.pwdPattern = /^(?=.*\d)(?=.*[a-z])(?=.*[~!@#$%^&*(){}|:"<>?[\];',./`]).{8,}$/;
    $scope.dialogActionLoading = false;
    $scope.saveNewPassword = function () {
      $scope.dialogActionLoading = true;
      UserService.changePwd($scope.changePwdUser, function () {
        $modalInstance.dismiss('cancel');
        SweetAlert.swal('Change password successfully', null, 'success');
        $scope.dialogActionLoading = false;
        $scope.logout();
      }, function (e) {
        $scope.dialogActionLoading = false;
        ResponseUtil.handleError(e);
      });
    };
    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    }
  };
  var initChangePwdUserWithCurrent = function () {
    return {
      username: $scope.currentUser.username,
      password: '',
      repeatPassword: '',
      newPassword: ''
    }
  };
  $scope.changePwd = function () {
    $scope.changePwdUser = initChangePwdUserWithCurrent();
    $scope.currentPwdNeeded = true;
    $modal.open({
      templateUrl: 'changePwd.html',
      controller: changePwdCtrl,
      scope: $scope
    });
  };

  AuthenticationService.ping(function (data) {
    $scope.currentUser = data.userDetails;
  });

  kylinConfig.init().$promise.then(function() {
    $scope.securityType = kylinConfig.getSecurityType();
    $scope.allowUseUserAndGroupModule = ['testing', 'custom'].indexOf($scope.securityType) >= 0;
  })
});

