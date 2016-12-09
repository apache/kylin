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

KylinApp.controller('CubeModelCtrl', function ($location,$scope, $modal,cubeConfig,MetaModel,SweetAlert,ModelGraphService,$log,TableModel,ModelService,loadingRequest,modelsManager,VdmUtil) {
    $scope.modelsManager = modelsManager;

    $scope.buildGraph = function (model) {
//        var newModel = jQuery.extend(true, {}, model);
        var newModel = angular.copy(model);
        ModelGraphService.buildTree(newModel);
    };

    $scope.cleanStatus = function(model){
      var _model = angular.copy(model);

        if (!_model)
        {
            return;
        }
        var newModel = jQuery.extend(true, {}, _model);
        delete newModel.project;
        delete  newModel.accessEntities;
        delete  newModel.visiblePage;
        delete  newModel.cubes;

        return angular.toJson(newModel,true);
    };

    $scope.cubeConfig = cubeConfig;

});
