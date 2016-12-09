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
    $scope.init = function (){
      $scope.rootFactTable=$scope.modelsManager.selectedModel.fact_table;
      $scope.aliasTableMap[VdmUtil.removeNameSpace($scope.modelsManager.selectedModel.fact_table)]=$scope.modelsManager.selectedModel.fact_table;
      $scope.tableAliasMap[$scope.modelsManager.selectedModel.fact_table]=VdmUtil.removeNameSpace($scope.modelsManager.selectedModel.fact_table);
      $scope.aliasName.push(VdmUtil.removeNameSpace($scope.modelsManager.selectedModel.fact_table));
      angular.forEach($scope.modelsManager.selectedModel.lookups,function(joinTable){
         $scope.aliasTableMap[joinTable.alias]=joinTable.table;
         $scope.tableAliasMap[joinTable.table]=joinTable.alias;
         $scope.aliasName.push(joinTable.alias);
      });
    }
    if($scope.state.mode=='edit'){
      $scope.init();
    }

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

    $scope.editLookup = function (lookup) {
        $scope.lookupState.editingIndex = lookupList.indexOf(lookup);
        $scope.lookupState.editing = true;

        // Make a copy of model will be editing.
        $scope.newLookup = angular.copy(lookup);
        $scope.newLookup.joinTable=VdmUtil.getNameSpaceTopName($scope.newLookup.join.foreign_key[0]);
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
        $scope.aliasTableMap[VdmUtil.removeNameSpace($scope.modelsManager.selectedModel.fact_table)]=$scope.modelsManager.selectedModel.fact_table;
        $scope.aliasName=[VdmUtil.removeNameSpace($scope.modelsManager.selectedModel.fact_table)];
        angular.copy($scope.newLookup, lookupList[$scope.lookupState.editingIndex]);
        angular.forEach(lookupList,function(joinTable){
          $scope.aliasName.push(joinTable.alias);
          $scope.aliasTableMap[joinTable.alias]=joinTable.table;
         // $scope.tableAliasMap[joinTable.alias]=joinTable.table;
        });

        $scope.resetParams();
    };
    $scope.changeFactTable = function () {
        delete $scope.aliasTableMap[VdmUtil.removeNameSpace($scope.modelsManager.selectedModel.fact_table)];
        $scope.aliasTableMap[VdmUtil.removeNameSpace($scope.rootFactTable)]=$scope.rootFactTable;
        delete $scope.tableAliasMap[$scope.modelsManager.selectedModel.fact_table];
        $scope.tableAliasMap[$scope.rootFactTable]=VdmUtil.removeNameSpace($scope.rootFactTable);
        $scope.aliasName.splice($scope.aliasName.indexOf(VdmUtil.removeNameSpace($scope.modelsManager.selectedModel.fact_table)),1);
        $scope.aliasName.push(VdmUtil.removeNameSpace($scope.rootFactTable));
        $scope.modelsManager.selectedModel.fact_table=$scope.rootFactTable;
    }
    $scope.changeJoinTable = function () {
        $scope.newLookup.alias=$scope.newLookup.table;
    }
    $scope.cancelLookup = function () {
        $scope.resetParams();
    };

    $scope.removeLookup = function (lookup) {
        var dimExist = _.some(modelsManager.selectedModel.dimensions,function(item,index){
            return item.alias===lookup.alias;
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
                        if (modelsManager.selectedModel.dimensions[i].alias === lookup.alias) {
                            modelsManager.selectedModel.dimensions.splice(i, 1);
                        }
                    }
                    lookupList.splice(lookupList.indexOf(lookup), 1);
                }
            });
        }else{
            lookupList.splice(lookupList.indexOf(lookup), 1);
        }
    };
    $scope.changeAlias = function (){
    }

    $scope.changeKey = function(index){
         var join_table = $scope.newLookup.jointable;
         var lookup_table = $scope.newLookup.table;
         var pk_column = $scope.newLookup.join.primary_key[index];
         var fk_column = $scope.newLookup.join.foreign_key[index];
         if(pk_column!=='null'&&fk_column!=='null'){
             $scope.newLookup.join.pk_type[index] = TableModel.getColumnType(pk_column,lookup_table);
             $scope.newLookup.join.fk_type[index] = TableModel.getColumnType(fk_column,join_table);
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
            // null validate
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

            var errorInfo = "";
            angular.forEach(errors,function(item){
                errorInfo+="\n"+item;
            });
            if(errors.length){
//                SweetAlert.swal('Warning!', errorInfo, '');
                SweetAlert.swal('', errorInfo, 'warning');
                return false;
            }else{
                return true;
            }

    };


});
