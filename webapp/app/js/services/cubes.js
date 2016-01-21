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

KylinApp.factory('CubeService', ['$resource', function ($resource, config) {
  return $resource(Config.service.url + 'cubes/:cubeId/:propName/:propValue/:action', {}, {
    list: {method: 'GET', params: {}, isArray: true},
    getCube: {method: 'GET', params: {}, isArray: false},
    getSql: {method: 'GET', params: {propName: 'segs', action: 'sql'}, isArray: false},
    updateNotifyList: {method: 'PUT', params: {propName: 'notify_list'}, isArray: false},
    cost: {method: 'PUT', params: {action: 'cost'}, isArray: false},
    rebuildLookUp: {method: 'PUT', params: {propName: 'segs', action: 'refresh_lookup'}, isArray: false},
    rebuildCube: {method: 'PUT', params: {action: 'rebuild'}, isArray: false},
    disable: {method: 'PUT', params: {action: 'disable'}, isArray: false},
    enable: {method: 'PUT', params: {action: 'enable'}, isArray: false},
    purge: {method: 'PUT', params: {action: 'purge'}, isArray: false},
    clone: {method: 'PUT', params: {action: 'clone'}, isArray: false},
    drop: {method: 'DELETE', params: {}, isArray: false},
    save: {method: 'POST', params: {}, isArray: false},
    update: {method: 'PUT', params: {}, isArray: false},
    getHbaseInfo: {method: 'GET', params: {propName: 'hbase'}, isArray: true}
  });
}]);
