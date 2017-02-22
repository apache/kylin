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

KylinApp.controller('ModelDataModelCtrl', function ($location,$scope, $modal,cubeConfig,MetaModel,SweetAlert,ModelGraphService,$log,TableModel,ModelService,loadingRequest,modelsManager,VdmUtil) {
    $scope.modelsManager = modelsManager;
    $scope.VdmUtil = VdmUtil;
    angular.forEach($scope.modelsManager.selectedModel.lookups,function(joinTable){
      if(!joinTable.alias){
        joinTable.alias=VdmUtil.removeNameSpace(joinTable.table);
      }
    });
    $scope.FactTable={root:$scope.modelsManager.selectedModel.fact_table};
    $scope.cubeConfig = cubeConfig;
    var DataModel = function () {
        return {
            name: '',
            fact_table: '',
            lookups: []
        };
    };

    var Lookup = function () {
        return {
            table: '',
            alias: '',
            joinTable:'',
            kind:'LOOKUP',
            join: {
                type: '',
                primary_key: [],
                foreign_key: [],
                isCompatible:[],
                pk_type:[],
                fk_type:[]
            }
        };
    };

    // Initialize params.
    $scope.lookupState = {
        editing: false,
        editingIndex: -1,
        filter: ''
    };

    $scope.newLookup = Lookup();

    var lookupList = modelsManager.selectedModel.lookups;

    $scope.openLookupModal = function () {
        var modalInstance = $modal.open({
            templateUrl: 'dataModelLookupTable.html',
            controller: cubeModelLookupModalCtrl,
            backdrop: 'static',
            scope: $scope
        });
        modalInstance.result.then(function () {
            if (!$scope.lookupState.editing) {
                $scope.doneAddLookup();
            } else {
                $scope.doneEditLookup();
            }

        }, function () {
            $scope.cancelLookup();
        });
    };

    // Controller for cube model lookup modal.
    var cubeModelLookupModalCtrl = function ($scope, $modalInstance) {
        $scope.ok = function () {
            $modalInstance.close();
        };

        $scope.cancel = function () {
            $modalInstance.dismiss('cancel');
        };
    };

    $scope.$watch('newLookup.alias',function(newValue,oldValue){
      for(var i=0;i<$scope.newLookup.join.primary_key.length;i++){
          $scope.newLookup.join.primary_key[i] = $scope.newLookup.join.primary_key[i].replace(/^.*?\./,newValue+'.');
        }
    });
    $scope.editLookup = function (lookup) {
        $scope.lookupState.editingIndex = lookupList.indexOf(lookup);
        $scope.lookupState.editing = true;
        // Make a copy of model will be editing.
        $scope.newLookup = angular.copy(lookup);
        $scope.newLookup.join.pk_type = [];
        $scope.newLookup.join.fk_type = [];
        $scope.newLookup.join.isCompatible=[];
        $scope.newLookup.joinTable=VdmUtil.getNameSpaceTopName($scope.newLookup.join.foreign_key[0]);
        angular.forEach($scope.newLookup.join.primary_key,function(pk,index){
            $scope.newLookup.join.pk_type[index] = TableModel.getColumnType(VdmUtil.removeNameSpace(pk),$scope.newLookup.table);
            $scope.newLookup.join.fk_type[index] = TableModel.getColumnType(VdmUtil.removeNameSpace($scope.newLookup.join.foreign_key[index]),$scope.aliasTableMap[$scope.newLookup.joinTable]);
            if($scope.newLookup.join.pk_type[index]!==$scope.newLookup.join.fk_type[index]){
               $scope.newLookup.join.isCompatible[index]=false;
            }else{
               $scope.newLookup.join.isCompatible[index]=true;
            }
        });
        $scope.openLookupModal();
    };

    $scope.doneAddLookup = function () {
        // Push newLookup which bound user input data.
        $scope.aliasTableMap[$scope.newLookup.alias]=$scope.newLookup.table;
        $scope.tableAliasMap[$scope.newLookup.table]=$scope.newLookup.alias;
        $scope.aliasName.push($scope.newLookup.alias);
        lookupList.push(angular.copy($scope.newLookup));
        $scope.resetParams();
    };

    $scope.doneEditLookup = function () {
        // Copy edited model to destination model.
        var oldAlias=$scope.aliasName[$scope.lookupState.editingIndex+1];
        var newAlias=$scope.newLookup.alias;
        if(oldAlias!=newAlias){
          $scope.aliasName[$scope.lookupState.editingIndex+1]=newAlias;
          for(var i=0;i<$scope.newLookup.join.primary_key.length;i++){
            $scope.newLookup.join.primary_key[i]=$scope.newLookup.join.primary_key[i].replace(oldAlias+'.',newAlias+'.');
          }

          delete $scope.aliasTableMap[oldAlias];
          $scope.aliasTableMap[newAlias]=$scope.newLookup.table;

          for(var i=0;i<lookupList.length;i++){
            for(var j=0;j<lookupList[i].join.foreign_key.length;j++){
             lookupList[i].join.foreign_key[j]=lookupList[i].join.foreign_key[j].replace(oldAlias+'.',newAlias+'.');
            }
          }

          for(var i=0;i<modelsManager.selectedModel.dimensions.length;i++){
            if(modelsManager.selectedModel.dimensions[i].table==oldAlias){
              modelsManager.selectedModel.dimensions[i].table=newAlias;
            }
          }

          if($scope.newLookup.kind=="FACT"){
            for(var i=0;i< modelsManager.selectedModel.metrics.length;i++){
               modelsManager.selectedModel.metrics[i]= modelsManager.selectedModel.metrics[i].replace(oldAlias+'.',newAlias+'.');
            }
            if(modelsManager.selectedModel.partition_desc.partition_date_column){
              modelsManager.selectedModel.partition_desc.partition_date_column = modelsManager.selectedModel.partition_desc.partition_date_column.replace(oldAlias+'.',newAlias+'.');
            }
          }
        }
        angular.copy($scope.newLookup,lookupList[$scope.lookupState.editingIndex]);

        $scope.resetParams();
    };
    $scope.changeFactTable = function () {
        if(!$scope.FactTable){
         return;
        }
        $scope.aliasTableMap[VdmUtil.removeNameSpace($scope.FactTable.root)]=$scope.FactTable.root;
        $scope.tableAliasMap[$scope.FactTable.root]=VdmUtil.removeNameSpace($scope.FactTable.root);
        $scope.aliasName.splice(0,$scope.aliasName.length);
        $scope.aliasName.push(VdmUtil.removeNameSpace($scope.FactTable.root));
        modelsManager.selectedModel.lookups.splice(0,modelsManager.selectedModel.lookups.length);
        modelsManager.selectedModel.dimensions.splice(0,modelsManager.selectedModel.dimensions.length);
        modelsManager.selectedModel.metrics.splice(0,modelsManager.selectedModel.metrics.length);
        modelsManager.selectedModel.partition_desc.partition_date_column = null;
        $scope.modelsManager.selectedModel.fact_table=$scope.FactTable.root;
    }
    $scope.changeJoinTable = function () {
        $scope.newLookup.alias=VdmUtil.removeNameSpace($scope.newLookup.table);
    }
    $scope.cancelLookup = function () {
        $scope.resetParams();
    };

    $scope.removeLookup = function (lookup) {
        var dimExist = _.some(modelsManager.selectedModel.dimensions,function(item,index){
            return item.table===lookup.alias;
        });
        if(dimExist) {
            SweetAlert.swal({
                 title: '',
                 text: "Once it's removed, all relative dimensions will be removed. Are you sure to remove the lookup table?",
                 type: '',
                 showCancelButton: true,
                 confirmButtonColor: '#DD6B55',
                 confirmButtonText: "Yes",
                 closeOnConfirm: true
            }, function (isConfirm) {
                if (isConfirm) {
                    for (var i = modelsManager.selectedModel.dimensions.length - 1; i >= 0; i--) {
                        if (modelsManager.selectedModel.dimensions[i].table === lookup.alias) {
                            modelsManager.selectedModel.dimensions.splice(i, 1);
                        }
                    }
                    delete $scope.aliasTableMap[lookup.alias];
                    lookupList.splice(lookupList.indexOf(lookup), 1);
                    $scope.aliasName.splice($scope.aliasName.indexOf(lookup.alias),1);
                }
            });
        }else{
            delete $scope.aliasTableMap[lookup.alias];
            lookupList.splice(lookupList.indexOf(lookup), 1);
            $scope.aliasName.splice($scope.aliasName.indexOf(lookup.alias),1);
        }
        console.log($scope.aliasName);
    };

    $scope.changeKey = function(index){
         var join_table = $scope.newLookup.joinTable;
         var lookup_table = $scope.newLookup.table;
         var pk_column = $scope.newLookup.join.primary_key[index];
         var fk_column = $scope.newLookup.join.foreign_key[index];
         if(pk_column!=='null'&&fk_column!=='null'){
            $scope.newLookup.join.pk_type[index] = TableModel.getColumnType(VdmUtil.removeNameSpace(pk_column),$scope.newLookup.table);
            $scope.newLookup.join.fk_type[index] = TableModel.getColumnType(VdmUtil.removeNameSpace(fk_column),$scope.aliasTableMap[$scope.newLookup.joinTable]);
            if($scope.newLookup.join.pk_type[index]!==$scope.newLookup.join.fk_type[index]){
               $scope.newLookup.join.isCompatible[index]=false;
            }else{
               $scope.newLookup.join.isCompatible[index]=true;
            }

         }
    }

    $scope.addNewJoin = function(){
        $scope.newLookup.join.primary_key.push("null");
        $scope.newLookup.join.foreign_key.push("null");
        $scope.newLookup.join.fk_type.push("null");
        $scope.newLookup.join.pk_type.push("null");
        $scope.newLookup.join.isCompatible.push(true);
    };

    $scope.removeJoin = function($index){
        $scope.newLookup.join.primary_key.splice($index,1);
        $scope.newLookup.join.foreign_key.splice($index,1);
        $scope.newLookup.join.fk_type.splice($index,1);
        $scope.newLookup.join.pk_type.splice($index,1);
        $scope.newLookup.join.isCompatible.splice($index,1);
    };

    $scope.resetParams = function () {
        $scope.lookupState.editing = false;
        $scope.lookupState.editingIndex = -1;
        $scope.newLookup = Lookup();
    };

    $scope.checkLookupForm = function(){
      var errors = [];
      for(var i = 0;i<$scope.newLookup.join.primary_key.length;i++){
        if($scope.newLookup.join.primary_key[i]==='null'){
          errors.push("Primary Key can't be null.");
          break;
        }
      }
      for(var i = 0;i<$scope.newLookup.join.foreign_key.length;i++){
        if($scope.newLookup.join.foreign_key[i]==='null'){
          errors.push("Foreign Key can't be null.");
          break;
        }
      }
      if($scope.aliasName.indexOf($scope.newLookup.alias)!=-1){
        if($scope.lookupState.editingIndex==-1){
           errors.push("Table Alias ["+$scope.newLookup.alias+"] already exist!");
        }else{
          if($scope.aliasName[$scope.lookupState.editingIndex+1] != $scope.newLookup.alias){
            errors.push("Table Alias ["+$scope.newLookup.alias+"] already exist!");
          }
        }
      }
      var errorInfo = "";
      angular.forEach(errors,function(item){
        errorInfo+="\n"+item;
      });
      if(errors.length){
        SweetAlert.swal('', errorInfo, 'warning');
        return false;
      }else{
        return true;
      }
    };
});
