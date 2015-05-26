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

KylinApp.service('modelsManager',function(ModelService,CubeService,$q,AccessService,ProjectModel,$log,CubeDescService,SweetAlert,cubesManager){
    var _this = this;
    this.models=[];

    //tracking models loading status
    this.loading = false;
    this.modelTreeData = [];
    this.selectedModel={};

    this.cubeModel={};
    this.cubeSelected = false;

    //list models and complemete cube,access info
    this.list = function(queryParam){

        var defer = $q.defer();
        var cubeDetail = [];
        var modelPermission = [];
        ModelService.list(queryParam, function (_models) {
            _this.removeAll();
            _this.loading = true;

            angular.forEach(_models, function (model, index) {
                $log.info("Add model permission info");
                modelPermission.push(
                AccessService.list({type: "DataModelDesc", uuid: model.uuid}, function (accessEntities) {
                    model.accessEntities = accessEntities;
                }).$promise
                )
                $log.info("Add cube info to model ,not detail info");
                cubeDetail.push(
//                    CubeService.list({offset: 0, limit: 70,modelName:model.name}, function (_cubes) {
                    CubeService.list({modelName:model.name}, function (_cubes) {
                    model.cubes = _cubes;
                    }).$promise
                );

                model.project = ProjectModel.getProjectByCubeModel(model.name);
            });
            $q.all(cubeDetail,modelPermission).then(
                function(result){
                    _models = _.filter(_models,function(models){return models.name!=undefined});
                    _this.models = _this.models.concat(_models);
                    defer.resolve(_this.models);
                }
            );
        },function(){
            defer.reject("Failed to load models");
        });
        return defer.promise;

    };

    //generator tree data info
    this.generatorTreeData = function(queryParam){
        var defer = $q.defer();
        _this.list(queryParam).then(function(resp){
            _this.modelTreeData = [];
            angular.forEach(_this.models,function(model){
                var _model = {
                    label:model.name,
                    noLeaf:true,
                    data:model,
                    onSelect:function(branch){
                        // set selected model
                        _this.selectedModel = branch.data;
                        _this.cubeSelected = false;
                    }
                };
                var _children = [];
                angular.forEach(model.cubes,function(cube){
                    _children.push(
                        {
                            label:cube.name,
                            data:cube,
                            onSelect:function(branch){
                                $log.info("cube selected:"+branch.data.name);
                                _this.cubeSelected = true;
//                                    $scope.cubeMetaFrame = branch.data;
//                                _this.selectedCube = branch.data;
                                cubesManager.currentCube = branch.data;
                                _this.listAccess(cubesManager.currentCube, 'CubeInstance');

                                CubeDescService.get({cube_name: cube.name}, {}, function (detail) {
                                    if (detail.length > 0&&detail[0].hasOwnProperty("name")) {
                                        //cubeMetaFrame for cube view and edit
//                                        $scope.cubeMetaFrame = detail[0];
                                        //for show detail info
//                                        $scope.cube.detail = detail[0];
                                        //add model info
                                        cubesManager.currentCube.detail = detail[0];
                                        cubesManager.cubeMetaFrame = detail[0];
                                        _this.cubeModel = _this.getModelByCube(cubesManager.currentCube.name);
                                    }else{
                                        SweetAlert.swal('Oops...', "No cube detail info loaded.", 'error');
                                    }
                                }, function (e) {
                                    if(e.data&& e.data.exception){
                                        var message =e.data.exception;
                                        var msg = !!(message) ? message : 'Failed to take action.';
                                        SweetAlert.swal('Oops...', msg, 'error');
                                    }else{
                                        SweetAlert.swal('Oops...', "Failed to take action.", 'error');
                                    }
                                });
                                // set selecte model
                            }
                        }
                    );
                });
                if(_children.length){
                    _model.children = _children;
                }
                _this.modelTreeData.push(_model);
            });
            _this.modelTreeData = _.sortBy(_this.modelTreeData, function (i) { return i.label.toLowerCase(); });
            defer.resolve(_this.modelTreeData);
        });
        return defer.promise;
    };


    this.removemodels = function(models){
        var modelsIndex = _this.models.indexOf(models);
        if (modelsIndex > -1) {
            _this.models.splice(modelsIndex, 1);
        }
    }

    this.getModel = function(modelName){
      return  _.find(_this.models,function(unit){
            return unit.name == modelName;
        })
    }

    this.getModels = function(){
        return _this.models;
    }

    this.getModelByCube = function(cubeName){
        return  _.find(_this.models,function(model){
            return _.some(model.cubes,function(_cube){
                return _cube.name == cubeName;
            });
        })
    }

    this.removeAll = function(){
        _this.models = [];
        _this.modelTreeData = [];
    };

    this.listAccess = function (entity, type) {
        var defer = $q.defer();

        entity.accessLoading = true;
        AccessService.list({type: type, uuid: entity.uuid}, function (accessEntities) {
            entity.accessLoading = false;
            entity.accessEntities = accessEntities;
            defer.resolve();
        });

        return defer.promise;
    };


});
