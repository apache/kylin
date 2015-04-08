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

KylinApp.controller('ModelsCtrl', function ($scope, $q, $routeParams, $location, $window,$modal, MessageService, CubeDescService, CubeService, JobService, UserService,  ProjectService,SweetAlert,loadingRequest,$log,modelConfig,ProjectModel,ModelService,MetaModel,modelsManager,cubesManager) {

        //tree data

        $scope.cubeSelected = false;
        $scope.cube = {};

        $scope.showModels=true;

        $scope.toggleTab = function(showModel){
            $scope.showModels = showModel;
        }

        $scope.modelsManager = modelsManager;
        $scope.cubesManager = cubesManager;
        $scope.modelConfig = modelConfig;
        modelsManager.removeAll();
        $scope.loading = false;
        $scope.action = {};
        $scope.window = 0.68 * $window.innerHeight;
        $scope.listParams={
            cubeName: $routeParams.cubeName,
            projectName: $routeParams.projectName
        };


        //  TODO offset&limit
        $scope.list = function () {
            var defer = $q.defer();
            if(!$scope.projectModel.projects.length){
                defer.resolve([]);
                return defer.promise;
            }

            var queryParam = {};
            if ($scope.listParams.modelName) {
                queryParam.modelName = $scope.listParams.modelName;
            }
            queryParam.projectName = $scope.projectModel.selectedProject;

            $scope.loading = true;

             modelsManager.list(queryParam).then(function(resp){
                $scope.loading = false;
                defer.resolve(resp);
            },function(resp){
                $scope.loading = false;
                defer.resolve([]);
            });

            return  defer.promise;
        };

        //add ref for selectedModel
//        $scope.model = modelsManager.selectedModel;
//        $scope.cubeSelected = modelsManager.cubeSelected;
//        $scope.cube = modelsManager.selectedCube;
//        $scope.cubeMetaFrame =modelsManager.cubeDetail;
//        $scope.cube={detail: modelsManager.cubeDetail};
//        $scope.metaModel = {model:modelsManager.curModel};

        $scope.init = function(){

            var queryParam = {};
            if ($scope.listParams.modelName) {
                queryParam.modelName = $scope.listParams.modelName;
            }
            queryParam.projectName = $scope.projectModel.selectedProject;

            modelsManager.generatorTreeData(queryParam).then(function(resp){
//                $scope.models_treedata = resp;
            });

        };

        $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
                modelsManager.removeAll();
                //init selected model
//                $scope.model = {};
                modelsManager.selectedModel;
                $scope.init();

        });


    $scope.status = {
        isopen: true
    };

    $scope.toggled = function(open) {
        $log.log('Dropdown is now: ', open);
    };

    $scope.toggleDropdown = function($event) {
        $event.preventDefault();
        $event.stopPropagation();
        $scope.status.isopen = !$scope.status.isopen;
    };


    });
