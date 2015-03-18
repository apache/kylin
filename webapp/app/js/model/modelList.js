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

KylinApp.service('ModelList',function(ModelService,$q,AccessService){
    var models=[];
    var _this = this;

    this.list = function(queryParam){

        var defer = $q.defer();
        ModelService.list(queryParam, function (_models) {
            angular.forEach(_models, function (model, index) {
                AccessService.list({type: "DataModelDesc", uuid: model.uuid}, function (accessEntities) {
                    model.accessEntities = accessEntities;
                });
            });
            _models = _.filter(_models,function(models){return models.name!=undefined});
            _this.models = _this.models.concat(_models);
            defer.resolve(_this.models.length);
        },function(){
            defer.reject("Failed to load models");
        });
        return defer.promise;

    };

    this.removemodels = function(models){
        var modelsIndex = _this.models.indexOf(models);
        if (modelsIndex > -1) {
            _this.models.splice(modelsIndex, 1);
        }
    }

    this.removeAll = function(){
        _this.models=[];
    };
});
