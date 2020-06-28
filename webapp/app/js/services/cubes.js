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
  function transformCuboidsResponse(data) {
    var cuboids = {
      nodeInfos: [],
      treeNode: data.root,
      totalRowCount: 0
    };
    function iterator(node, parentRowCount) {
      node.parent_row_count = parentRowCount;
      cuboids.nodeInfos.push(node);
      cuboids.totalRowCount += node.row_count;
      if (node.children.length) {
        angular.forEach(node.children, function(child) {
          iterator(child, node.row_count);
        });
      }
    };
    if (data.root) {
      iterator(data.root, data.root.row_count);
    }
    return cuboids;
  };
  return $resource(Config.service.url + 'cubes/:cubeId/:propName/:propValue/:action', {}, {
    list: {method: 'GET', params: {}, isArray: true},
    getValidEncodings: {method: 'GET', params: {action:"validEncodings"}, isArray: false},
    getCube: {method: 'GET', params: {}, isArray: false},
    getSql: {method: 'GET', params: {action: 'sql'}, isArray: false},
    updateNotifyList: {method: 'PUT', params: {propName: 'notify_list'}, isArray: false},
    updateOwner: {method: 'PUT', params: {propName: 'owner'}, isArray: false},
    cost: {method: 'PUT', params: {action: 'cost'}, isArray: false},
    rebuildLookUp: {method: 'PUT', params: {propName: 'segs', action: 'refresh_lookup'}, isArray: false},
    rebuildCube: {method: 'PUT', params: {action: 'rebuild'}, isArray: false},
    rebuildStreamingCube: {method: 'PUT', params: {action: 'build2'}, isArray: false},
    disable: {method: 'PUT', params: {action: 'disable'}, isArray: false},
    enable: {method: 'PUT', params: {action: 'enable'}, isArray: false},
    purge: {method: 'PUT', params: {action: 'purge'}, isArray: false},
    clone: {method: 'PUT', params: {action: 'clone'}, isArray: false},
    deleteSegment: {method: 'DELETE', params: {propName: 'segs'}, isArray: false},
    drop: {method: 'DELETE', params: {}, isArray: false},
    save: {method: 'POST', params: {}, isArray: false},
    update: {method: 'PUT', params: {}, isArray: false},
    getStorageInfo: {method: 'GET', params: {propName: 'storage'}, isArray: true},
    getCurrentCuboids: {
      method: 'GET',
      params: {
          propName: 'cuboids',
          propValue: 'current'
      },
      isArray: false,
      interceptor: {
        response: function(response) {
          return transformCuboidsResponse(response.data);
        }
      }
    },
    getRecommendCuboids: {
      method: 'GET',
      params: {propName: 'cuboids', propValue: 'recommend'},
      isArray: false,
      interceptor: {
        response: function(response) {
          return transformCuboidsResponse(response.data);
        }
      }
    },
    optimize: {method: 'PUT', params: {action: 'optimize'}, isArray: false},
    autoMigrate: {method: 'POST', params: {action: 'migrate'}, isArray: false},
    lookupRefresh: {method: 'PUT', params: {action: 'refresh_lookup'}, isArray: false},
    checkDuplicateCubeName: {method: 'GET', params: {action: 'validate'}, isArray: false}
  });
}]);
