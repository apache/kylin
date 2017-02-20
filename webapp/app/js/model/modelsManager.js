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

KylinApp.service('modelsManager',function(ModelService,CubeService,$q,AccessService,ProjectModel,VdmUtil,TableModel){
    var _this = this;
    this.models=[];
    this.modelNameList = [];

    //tracking models loading status
    this.loading = false;
    this.selectedModel={};

    //list models
    this.list = function(queryParam){

        _this.loading = true;
        var defer = $q.defer();
        ModelService.list(queryParam, function (_models) {

            angular.forEach(_models, function (model, index) {
              _this.modelNameList.push(model.name);
              model.project = ProjectModel.getProjectByCubeModel(model.name);
            });

            _models = _.filter(_models,function(models){return models.name!=undefined});
            _this.models = _models;
            _this.loading = false;

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
        _this.selectedModel = {};
        _this.modelNameList = [];
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
    //init alias map
    this.tableAliasMap={};
    this.aliasTableMap={};
    this.availableFactTables=[];
    this.availableLookupTables=[];
    this.aliasName=[];

    this.initAliasMapByModelSchema=function(metaModel){
      var rootFactTable = VdmUtil.removeNameSpace(metaModel.model.fact_table);
      this.tableAliasMap={};
      this.aliasTableMap={};
      this.availableFactTables=[];
      this.availableLookupTables=[];
      this.aliasName=[];
      this.availableFactTables.push(rootFactTable);
      this.aliasName.push(rootFactTable);
      this.aliasTableMap[rootFactTable]=metaModel.model.fact_table;
      this.tableAliasMap[metaModel.model.fact_table]=rootFactTable;
      var _this=this;
      angular.forEach(metaModel.model.lookups,function(joinTable){
        if(!joinTable.alias){
          joinTable.alias=VdmUtil.removeNameSpace(joinTable.table);
        }
        if(joinTable.kind=="FACT"){
          _this.availableFactTables.push(joinTable.alias);
        }else{
          _this.availableLookupTables.push(joinTable.alias);
        }
        _this.aliasTableMap[joinTable.alias]=joinTable.table;
        _this.tableAliasMap[joinTable.table]=joinTable.alias;
        _this.aliasName.push(joinTable.alias);
      });
    }
    this.getDatabaseByColumnName=function(column){
      return  VdmUtil.getNameSpaceTopName(this.aliasTableMap[VdmUtil.getNameSpaceTopName(column)])
    }
    this.getColumnTypeByColumnName=function(column){
      return TableModel.columnNameTypeMap[this.aliasTableMap[VdmUtil.getNameSpaceTopName(column)]+'.'+VdmUtil.removeNameSpace(column)];
    }
    this.getColumnsByAlias=function(alias){
      return TableModel.getColumnsByTable(this.aliasTableMap[alias]);
    }

});
