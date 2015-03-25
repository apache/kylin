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

KylinApp.controller('CubeModelCtrl', function ($scope, $modal,cubeConfig,MetaModel,SweetAlert,GraphService,$log) {


    $scope.buildGraph = function (model) {
//        var newModel = jQuery.extend(true, {}, model);
        var newModel = angular.copy(model);
        GraphService.buildTree(newModel);
    };

    $scope.cleanStatus = function(model){

        if (!model)
        {
            return;
        }
        var newModel = jQuery.extend(true, {}, model);
        delete newModel.project;
        delete  newModel.accessEntities;
        delete  newModel.visiblePage;
        delete  newModel.cubes;

        return newModel;
    };

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
            join: {
                type: '',
                primary_key: [],
                foreign_key: []
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

    var lookupList = $scope.model.lookups;

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

        $scope.openLookupModal();
    };

    $scope.doneAddLookup = function () {
        // Push newLookup which bound user input data.
        lookupList.push(angular.copy($scope.newLookup));

        $scope.resetParams();
    };

    $scope.doneEditLookup = function () {
        // Copy edited model to destination model.
        angular.copy($scope.newLookup, lookupList[$scope.lookupState.editingIndex]);

        $scope.resetParams();
    };

    $scope.cancelLookup = function () {
        $scope.resetParams();
    };

        $scope.removeLookup = function (lookup) {
            var dimExist = _.some($scope.model.dimensions,function(item,index){
                return item.table===lookup.table;
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
                        for (var i = $scope.model.dimensions.length - 1; i >= 0; i--) {
                            if ($scope.model.dimensions[i].table === lookup.table) {
                                $scope.model.dimensions.splice(i, 1);
                            }
                        }
                        lookupList.splice(lookupList.indexOf(lookup), 1);
                    }
                });
            }else{
                lookupList.splice(lookupList.indexOf(lookup), 1);
            }
        };


    $scope.addNewJoin = function(){
        $scope.newLookup.join.primary_key.push("null");
        $scope.newLookup.join.foreign_key.push("null");
    };

    $scope.removeJoin = function($index){
        $scope.newLookup.join.primary_key.splice($index,1);
        $scope.newLookup.join.foreign_key.splice($index,1);
    };

    $scope.resetParams = function () {
        $scope.lookupState.editing = false;
        $scope.lookupState.editingIndex = -1;
        $scope.newLookup = Lookup();
    };
});
