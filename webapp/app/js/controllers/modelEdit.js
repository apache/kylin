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


KylinApp.controller('ModelEditCtrl', function ($scope, $q, $routeParams, $location, $templateCache, $interpolate, MessageService, TableService, CubeDescService, ModelService, loadingRequest, SweetAlert,$log,cubeConfig,CubeDescModel,ModelDescService,MetaModel,TableModel,ProjectService,ProjectModel,modelsManager) {
    //add or edit ?
    var absUrl = $location.absUrl();
    $scope.modelMode = absUrl.indexOf("/models/add")!=-1?'addNewModel':absUrl.indexOf("/models/edit")!=-1?'editExistModel':'default';

    if($scope.modelMode=="addNewModel"&&ProjectModel.selectedProject==null){
        SweetAlert.swal('Oops...', 'Please select your project first.', 'warning');
        $location.path("/models");
    }

    $scope.modelsManager = modelsManager;

    $scope.cubeConfig = cubeConfig;

    $scope.getPartitonColumns = function(tableName){
        var columns = _.filter($scope.getColumnsByTable(tableName),function(column){
            return column.datatype==="date"||column.datatype==="timestamp"||column.datatype==="string"||column.datatype.startsWith("varchar")||column.datatype==="bigint"||column.datatype==="int"||column.datatype==="integer";
        });
        return columns;
    };

    $scope.getPartitonTimeColumns = function(tableName){
        var columns = _.filter($scope.getColumnsByTable(tableName),function(column){
            return column.datatype==="time"||column.datatype==="timestamp"||column.datatype==="string"||column.datatype.startsWith("varchar");
        });
        return columns;
    };

    $scope.getColumnsByTable = function (tableName) {
        var temp = [];
        angular.forEach(TableModel.selectProjectTables, function (table) {
            if (table.name == tableName) {
                temp = table.columns;
            }
        });
        return temp;
    };

    $scope.getColumnType = function (_column,table){
        var columns = $scope.getColumnsByTable(table);
        var type;
        angular.forEach(columns,function(column){
            if(_column === column.name){
                type = column.datatype;
                return;
            }
        });
        return type;
    };

    $scope.isBigInt=false;
    $scope.partitionChange = function (dateColumn) {
        if(dateColumn==null) {
            return;
        }
        var column = _.filter($scope.getColumnsByTable($scope.modelsManager.selectedModel.fact_table),function(_column){
            var columnName=$scope.modelsManager.selectedModel.fact_table+"."+_column.name;
            if(dateColumn==columnName)
               return _column;
        });
        if(column[0].datatype==="bigint"||column[0].datatype==="int"||column[0].datatype==="integer"){
           $scope.isBigInt=true;
           $scope.modelsManager.selectedModel.partition_desc.partition_date_format=null;;
           $scope.partitionColumn.hasSeparateTimeColumn=false;
           $scope.modelsManager.selectedModel.partition_desc.partition_time_column=null;
           $scope.modelsManager.selectedModel.partition_desc.partition_time_format=null;
        }
        else{
           $scope.isBigInt=false;
        }

    }

    // ~ Define data
    $scope.state = {
        "modelSchema": "",
        mode:'edit',
        modelName: $scope.routeParams.modelName,
        project:$scope.projectModel.selectedProject
    };

    $scope.partitionColumn ={
      "hasSeparateTimeColumn" : false
    }
    // ~ init
    if ($scope.isEdit = !!$routeParams.modelName) {

        var modelName = $routeParams.modelName;
        ModelDescService.query({model_name: modelName}, function (model) {
                    if (model) {
                        modelsManager.selectedModel = model;
                        if(!$scope.modelsManager.selectedModel.partition_desc.partition_data_format){
                          $scope.isBigInt = true;
                        }
                        if($scope.modelsManager.selectedModel.partition_desc.partition_time_column){
                          $scope.partitionColumn.hasSeparateTimeColumn = true;
                        }
                        modelsManager.selectedModel.project = ProjectModel.getProjectByCubeModel(modelName);

                        if(!ProjectModel.getSelectedProject()){
                            ProjectModel.setSelectedProject(modelsManager.selectedModel.project);
                            TableModel.aceSrcTbLoaded();
                        }
                    }
                });
        //init project

    } else {
        MetaModel.initModel();
        modelsManager.selectedModel = MetaModel.getMetaModel();
        modelsManager.selectedModel.project = ProjectModel.getSelectedProject();
    }


    $scope.toggleHasSeparateColumn = function(){
      if($scope.partitionColumn.hasSeparateTimeColumn == false){
        $scope.modelsManager.selectedModel.partition_desc.partition_time_column = null;
      }
    }

    $scope.prepareModel = function () {
        // generate column family
        $scope.state.project = modelsManager.selectedModel.project;
        var _model = angular.copy(modelsManager.selectedModel);
        delete _model.project;
        $scope.state.modelSchema = angular.toJson(_model, true);

    };

    $scope.modelResultTmpl = function (notification) {
        // Get the static notification template.
        var tmpl = notification.type == 'success' ? 'modelResultSuccess.html' : 'modelResultError.html';
        return $interpolate($templateCache.get(tmpl))(notification);
    };

    $scope.saveModel = function () {

        $scope.prepareModel();

        try {
            angular.fromJson($scope.state.modelSchema);
        } catch (e) {
            SweetAlert.swal('Oops...', 'Invalid model json format..', 'error');
            return;
        }

        SweetAlert.swal({
            title: $scope.isEdit?'Are you sure to update the model?':"Are you sure to save the Model?",
            text: $scope.isEdit?' Please note: if model schema is changed, all cubes of the model will be affected.':'',
            type: 'warning',
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            confirmButtonText: "Yes",
            closeOnConfirm: true
        }, function(isConfirm) {
            if(isConfirm){
                loadingRequest.show();

                if ($scope.isEdit) {
                    ModelService.update({}, {modelDescData:$scope.state.modelSchema, modelName: $routeParams.modelName, project: $scope.state.project}, function (request) {
                        if (request.successful) {
                            $scope.state.modelSchema = request.modelSchema;
                            SweetAlert.swal('', 'Updated the model successfully.', 'success');
                            $location.path("/models");
                            //location.reload();
                        } else {
                               $scope.saveModelRollBack();
                                var message =request.message;
                                var msg = !!(message) ? message : 'Failed to take action.';
                                MessageService.sendMsg($scope.modelResultTmpl({'text':msg,'schema':$scope.state.modelSchema}), 'error', {}, true, 'top_center');
                        }
                        //end loading
                        loadingRequest.hide();
                    }, function (e) {
                        $scope.saveModelRollBack();

                        if(e.data&& e.data.exception){
                            var message =e.data.exception;
                            var msg = !!(message) ? message : 'Failed to take action.';
                            $log.log($scope.modelResultTmpl({'text':msg,'schema':$scope.state.modelSchema}));
                            MessageService.sendMsg($scope.modelResultTmpl({'text':msg,'schema':$scope.state.modelSchema}), 'error', {}, true, 'top_center');
                        } else {
                            MessageService.sendMsg($scope.modelResultTmpl({'text':'Failed to take action.','schema':$scope.state.modelSchema}), 'error', {}, true, 'top_center');
                        }
                        loadingRequest.hide();
                    });
                } else {
                    ModelService.save({}, {modelDescData:$scope.state.modelSchema, project: $scope.state.project}, function (request) {
                        if(request.successful) {

                          $scope.state.modelSchema = request.modelSchema;
                          SweetAlert.swal('', 'Created the model successfully.', 'success');
                          $location.path("/models");
                          location.reload();
                        } else {
                            $scope.saveModelRollBack();
                            var message =request.message;
                            var msg = !!(message) ? message : 'Failed to take action.';
                            MessageService.sendMsg($scope.modelResultTmpl({'text':msg,'schema':$scope.state.modelSchema}), 'error', {}, true, 'top_center');
                        }

                        //end loading
                        loadingRequest.hide();
                    }, function (e) {
                        $scope.saveModelRollBack();

                        if (e.data && e.data.exception) {
                            var message =e.data.exception;
                            var msg = !!(message) ? message : 'Failed to take action.';
                            MessageService.sendMsg($scope.modelResultTmpl({'text':msg,'schema':$scope.state.modelSchema}), 'error', {}, true, 'top_center');
                        } else {
                            MessageService.sendMsg($scope.modelResultTmpl({'text':"Failed to take action.",'schema':$scope.state.modelSchema}), 'error', {}, true, 'top_center');
                        }
                        //end loading
                        loadingRequest.hide();

                    });
                }
            }
            else{
                $scope.saveModelRollBack();
            }
        });
    };

//    reverse the date
    $scope.saveModelRollBack = function (){
    };

    $scope.removeTableDimensions = function(tableIndex){
        modelsManager.selectedModel.dimensions.splice(tableIndex,1);
    }

    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
      if(!$scope.projectModel.getSelectedProject()) {
        return;
      }
      var param = {
            ext: true,
            project:newValue
        };
        if(newValue){
            TableModel.initTables();
            TableService.list(param, function (tables) {
                angular.forEach(tables, function (table) {
                    table.name = table.database+"."+table.name;
                    TableModel.addTable(table);
                });
            });
        }
    });
});
