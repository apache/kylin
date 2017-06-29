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

KylinApp.controller('ModelMeasuresCtrl', function ($scope, $modal,MetaModel,modelsManager,VdmUtil,$filter) {
    $scope.modelsManager = modelsManager;
    $scope.availableFactTables = [];
    $scope.selectedFactTables = {};
    $scope.availableFactTables.push(VdmUtil.removeNameSpace($scope.modelsManager.selectedModel.fact_table));
    var joinTable = $scope.modelsManager.selectedModel.lookups;
    for (var j = 0; j < joinTable.length; j++) {
        if(joinTable[j].kind=='FACT'){
          $scope.availableFactTables.push(joinTable[j].alias);
        }
    }
    $scope.changeColumns = function (table){
      angular.forEach($scope.selectedFactTables[table],function(column){
        if($scope.modelsManager.selectedModel.metrics.indexOf(column)==-1){
          $scope.modelsManager.selectedModel.metrics.push(column);
        }
      });
      angular.forEach($scope.modelsManager.selectedModel.metrics,function(metric){
        if($scope.selectedFactTables[VdmUtil.getNameSpaceAliasName(metric)].indexOf(metric)==-1){
          $scope.modelsManager.selectedModel.metrics.splice($scope.modelsManager.selectedModel.metrics.indexOf(metric),1);
        }
      });
    }
    angular.forEach($scope.modelsManager.selectedModel.metrics,function(metric){
       var aliasName = VdmUtil.getNameSpaceAliasName(metric)
       $scope.selectedFactTables[aliasName]=$scope.selectedFactTables[aliasName]||[];
       $scope.selectedFactTables[aliasName].push(metric);
    });
    for (var i in $scope.selectedFactTables) {
      $scope.selectedFactTables[i] = $filter('notInJoin')($scope.selectedFactTables[i], i, $scope.modelsManager.selectedModel.lookups)
      $scope.changeColumns(i)
    }
});
