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

KylinApp.controller('ModelsCtrl', function ($scope, $q, $routeParams, $location, $window,$modal, MessageService, CubeDescService, CubeService, JobService, UserService,  ProjectService,SweetAlert,loadingRequest,$log,modelConfig,ProjectModel,ModelService,MetaModel,ModelList) {
        //selected model
        $scope.model = {};
        //tree data
        $scope.models_treedata=[];
        $scope.selectedCubes = [];

        $scope.showModels=true;

        $scope.toggleTab = function(showModel){
            $scope.showModels = showModel;
            console.log($scope.showModels);
        }

        $scope.modelList = ModelList;
        $scope.modelConfig = modelConfig;
        ModelList.removeAll();
        $scope.loading = false;
        $scope.action = {};
        $scope.window = 0.68 * $window.innerHeight;
        $scope.listParams={
            cubeName: $routeParams.cubeName,
            projectName: $routeParams.projectName
        };


        //  TODO offset&limit
        $scope.list = function (offset, limit) {
            if(!$scope.projectModel.projects.length){
                return [];
            }
            offset = (!!offset) ? offset : 0;
            limit = (!!limit) ? limit : 70;

            var queryParam = {offset: offset, limit: limit};
            if ($scope.listParams.modelName) {
                queryParam.modelName = $scope.listParams.modelName;
            }
            queryParam.projectName = $scope.projectModel.selectedProject;

            $scope.loading = true;

            var defer = $q.defer();
             ModelList.list(queryParam).then(function(resp){
                $scope.loading = false;
                defer.resolve(resp);
            },function(resp){
                $scope.loading = false;
                defer.resolve([]);
            });

            return  defer.promise;
        };

        $scope.init = function(){
            $scope.list().then(function(resp){
                $scope.models_treedata = [];
                angular.forEach(ModelList.models,function(model){
                    var _model = {
                        label:model.name,
                        noLeaf:true,
                        data:model,
                        onSelect:function(branch){
                         // set selecte model
                            $scope.model=branch.data;
                            $scope.selectedCubes = branch.data.cubes;
                        }
                    };
                    var _children = [];
                    angular.forEach(model.cubes,function(cube){
                        _children.push(
                            {
                                label:cube.name,
                                data:cube,
                                onSelect:function(branch){
                                    console.log("cube selected:"+branch.data);
                                    // set selecte model
                                }
                            }
                        );
                    });
                    if(_children.length){
                         _model.children = _children;
                    }
                    $scope.models_treedata.push(_model);
                });
                $scope.models_treedata = _.sortBy($scope.models_treedata, function (i) { return i.label.toLowerCase(); });

            });
        };

        $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
                ModelList.removeAll();
                //init selected model
                $scope.model = {};
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
