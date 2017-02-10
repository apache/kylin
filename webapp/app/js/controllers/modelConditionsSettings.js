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

/**
 * Created by jiazhong on 2015/3/13.
 */

'use strict';

KylinApp.controller('ModelConditionsSettingsCtrl', function ($scope, $modal,MetaModel,modelsManager,VdmUtil) {
  $scope.modelsManager = modelsManager;
  $scope.availableFactTables = [];
  $scope.initSetting = function (){
    $scope.selectedTables={fact:VdmUtil.getNameSpaceAliasName($scope.modelsManager.selectedModel.partition_desc.partition_date_column)}
    $scope.selectedTablesForPartitionTime={fact:VdmUtil.getNameSpaceAliasName($scope.modelsManager.selectedModel.partition_desc.partition_time_column)}
    $scope.availableFactTables.push(VdmUtil.removeNameSpace($scope.modelsManager.selectedModel.fact_table));
    var joinTable = $scope.modelsManager.selectedModel.lookups;
    for (var j = 0; j < joinTable.length; j++) {
      if(joinTable[j].kind=='FACT'){
        $scope.availableFactTables.push(joinTable[j].alias);
      }
    }
  }

  $scope.isFormatEdit = {editable:false};
  var judgeFormatEditable = function(dateColumn){
    if(dateColumn == null){
      $scope.isFormatEdit.editable = false;
      return;
    }
    var column = _.filter($scope.getColumnsByAlias(VdmUtil.getNameSpaceAliasName(dateColumn)),function(_column){
      var columnName=VdmUtil.getNameSpaceAliasName(dateColumn)+"."+_column.name;
      if(dateColumn == columnName){
        return _column;
      }
    });

    var data_type = column[0].datatype;
    if(data_type ==="bigint" ||data_type ==="int" ||data_type ==="integer"){
      $scope.isFormatEdit.editable = false;
      $scope.modelsManager.selectedModel.partition_desc.partition_date_format='yyyyMMdd';
      $scope.partitionColumn.hasSeparateTimeColumn=false;
      $scope.modelsManager.selectedModel.partition_desc.partition_time_column=null;
      $scope.modelsManager.selectedModel.partition_desc.partition_time_format=null;

      return;
    }

    $scope.isFormatEdit.editable = true;
    return;

  };

  $scope.getPartitonColumns = function(alias){
    var columns = _.filter($scope.getColumnsByAlias(alias),function(column){
      return column.datatype==="date"||column.datatype==="timestamp"||column.datatype==="string"||column.datatype.startsWith("varchar")||column.datatype==="bigint"||column.datatype==="int"||column.datatype==="integer";
    });
    return columns;
  };

  $scope.getPartitonTimeColumns = function(tableName,filterColumn){
    var columns = _.filter($scope.getColumnsByAlias(tableName),function(column){
      return (column.datatype==="time"||column.datatype==="timestamp"||column.datatype==="string"||column.datatype.startsWith("varchar"))&&(tableName+'.'+column.name!=filterColumn);
    });
    return columns;
  };

  $scope.partitionChange = function (dateColumn) {
    judgeFormatEditable(dateColumn);
  };
  $scope.partitionColumn ={
      "hasSeparateTimeColumn" : false
  }

  if ($scope.state.mode=='edit'){
    $scope.initSetting();
    judgeFormatEditable($scope.modelsManager.selectedModel.partition_desc.partition_date_column);
  }
  if($scope.modelsManager.selectedModel.partition_desc.partition_time_column){
    $scope.partitionColumn.hasSeparateTimeColumn = true;
  }
  $scope.toggleHasSeparateColumn = function(){
    if($scope.partitionColumn.hasSeparateTimeColumn == false){
      $scope.modelsManager.selectedModel.partition_desc.partition_time_column = null;
    }
  }
});
