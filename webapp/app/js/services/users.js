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

KylinApp.service('UserService', function ($resource) {
    var curUser = {};

    this.getCurUser = function () {
        return curUser;
    };
    this.setCurUser = function (user) {
        curUser = user;
    };
    this.hasRole = function (role) {
        var hasRole = false;
        if (curUser.userDetails) {
            angular.forEach(curUser.userDetails.authorities, function (authority, index) {
                if (authority.authority == role) {
                    hasRole = true;
                }
            });
        }

        return hasRole;
    };
    this.isAuthorized = function () {
        return  curUser.userDetails && curUser.userDetails.authorities && curUser.userDetails.authorities.length > 0;
    };
    this.getHomePage = function () {
        return this.isAuthorized()? "/models" : "/login";
    }

    var apiService = $resource(Config.service.url + 'user/:action/:userName', {}, {
      listUsers: {method: 'GET', params: {action: 'users'}, isArray: false},
      delUser: {method: 'DELETE', isArray: false},
      addUser: {method: 'POST',  params: {} ,isArray: false},
      changePwd: {method: 'PUT', params: {action: 'password'}, isArray: false},
      updateUser: {method: 'PUT', params: {}, isArray: false},
      assignGroup: {method: 'PUT', params: {}, isArray: false}
    });
    angular.extend(this, apiService)
});
