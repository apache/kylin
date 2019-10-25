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

KylinApp.factory('UserGroupService', ['$resource', function ($resource, config) {
  return $resource(Config.service.url + 'user_group/:action/:group', {}, {
    listGroups: {method: 'GET', params: {action:'groups'}, isArray: false},
    addGroup: {method: 'POST', params: {}, isArray: false},
    delGroup: {method: 'DELETE', params: {}, isArray: false},
    editGroup: {method: 'PUT', params: {}, isArray: false},
    assignUsers: {method: 'PUT', params: {action: 'users'}, isArray: false},
    getUsersByGroup: {method: 'GET', params: {action:'users'}, isArray: false}
  });
}]);
