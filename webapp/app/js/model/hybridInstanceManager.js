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

KylinApp.service('hybridInstanceManager', function($q, HybridInstanceService, ProjectModel) {
  var _this = this;
  this.hybridInstances = [];
  this.hybridInstanceNameList = [];

  //tracking models loading status
  this.loading = false;

  //list hybrid cubes
  this.list = function(queryParam) {
    _this.loading = true;

    var defer = $q.defer();

    HybridInstanceService.list(queryParam, function(_hybridInstances) {
      _hybridInstances = _hybridInstances.map(function(_hybridInstance) {
        var instance = _hybridInstance.hybridInstance;
        instance.project = _hybridInstance.projectName;
        instance.model = _hybridInstance.modelName;

        return instance;
      });

      angular.forEach(_hybridInstances, function(hybridInstance) {
        _this.hybridInstanceNameList.push(hybridInstance.name);
        // hybridInstance.project = ProjectModel.getProjectByCubeModel(hybridInstance.name);
      });

      _hybridInstances = _.filter(_hybridInstances, function(hybridInstance) {
        return hybridInstance.name !== undefined;
      });

      _this.hybridInstances = _hybridInstances;
      _this.loading = false;
    },
    function() {
      defer.reject('Failed to load models');
    });

    return defer.promise;
  };
})
