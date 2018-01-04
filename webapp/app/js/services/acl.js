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

KylinApp.factory('AclService', ['$resource', function ($resource, config) {
  return $resource(Config.service.url + 'acl/:kind/:project/:type/:black/:tablename/:username', {}, {
    getTableAclList: {method: 'GET', params: {kind:'table'}, isArray: true},
    getTableAclBlackList : {method: 'GET', params: {kind:'table', black: 'black'}, isArray: true},
    saveAclSetOfTable : {method: 'POST', params: {kind:'table'}, isArray: true},
    cancelAclSetOfTable: {method: 'DELETE', params: {kind:'table'}, isArray: false}
  });
}]);
