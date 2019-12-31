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

KylinApp.controller('LoginCtrl', function ($scope, $rootScope, $location, $base64, $window, $log, AuthenticationService, UserService, ProjectService, ProjectModel, kylinConfig) {
  $scope.username = null;
  $scope.password = null;
  $scope.loading = false;

  $scope.authn = null;

  $scope.init = function () {
    if (!kylinConfig.isInitialized()) {
      kylinConfig.init().$promise.then(function (data) {
        $scope.initCustomAuthnMethods();
      });
    } else {
      $scope.initCustomAuthnMethods();
    }
  };

  $scope.initCustomAuthnMethods = function () {
    var profile = kylinConfig.getProperty('kylin.security.profile');
    var additionalProfiles = kylinConfig.getProperty('kylin.security.additional-profiles');
    if (profile !== 'custom' || !additionalProfiles) {
      return;
    }
    var additions = additionalProfiles.split(',');
    for (var i = 0; i < additions.length; i++) {
      var prof = additions[i].trim();
      if (!prof || !prof.startsWith('authn-'))
        continue;
      $scope.authn = $scope.createAuthnMethodInfo(prof.substr(6));
      break;
    }
  };

  $scope.createAuthnMethodInfo = function (method) {
    if (method === 'cas') {
      return $scope.createCASAuthnMethod();
    }
    if (method === 'saml') {
      return $scope.createSAMLAuthnMethod();
    }

    $log.error('Not supported authentication method');
    return null;
  };

  $scope.createCASAuthnMethod = function () {
    return {label: 'CAS Login', loginEntry: 'cas/login', singleSignOut: true, logoutEntry: 'cas/logout'}
  };

  $scope.createSAMLAuthnMethod = function () {
    return {label: 'SAML Login', loginEntry: 'saml/login', singleSignOut: true, logoutEntry: 'saml/logout'}
  };

  $scope.redirectLoginEntry = function (authn) {
    $window.location.href = authn.loginEntry;
  };

  $scope.login = function () {
    $rootScope.userAction.islogout = false;
    // set the basic authentication header that will be parsed in the next request and used to authenticate
    httpHeaders.common['Authorization'] = 'Basic ' + $base64.encode($scope.username + ':' + $scope.password);
    $scope.loading = true;

    AuthenticationService.login({}, {}, function (data) {
      $scope.loading = false;
      $rootScope.$broadcast('event:loginConfirmed');
      UserService.setCurUser(data);
      $location.path(UserService.getHomePage());
    }, function (error) {
      $scope.loading = false;
      $scope.error = error.status < 500
        ? "Unable to login, please check your username/password."
        : "System error, please contact your administrator.";
    });
  };

  $scope.init();
});
