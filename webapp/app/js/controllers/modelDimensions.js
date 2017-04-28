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

KylinApp.controller('ModelDimensionsCtrl', function ($scope, $modal,MetaModel,modelsManager,VdmUtil) {
    $scope.modelsManager = modelsManager;

    // Available columns list derived from cube data model.
    $scope.availableColumns = {};

    // Columns selected and disabled status bound to UI, group by table.
    $scope.selectedColumns = {};

    // Available tables cache: 1st is the fact table, next are lookup tables.
    $scope.availableTables = [];
    $scope.dimensions=[];

    // Dump available columns plus column table name, whether is from lookup table.
    $scope.initColumns = function () {

        $scope.availableTables.push(VdmUtil.removeNameSpace($scope.modelsManager.selectedModel.fact_table));
        var lookups = modelsManager.selectedModel.lookups;
        for (var j = 0; j < lookups.length; j++) {
            $scope.availableTables.push(lookups[j].alias);
        }

        for(var i = 0;i<$scope.availableTables.length;i++){
            var tableInUse = _.some(modelsManager.selectedModel.dimensions,function(item){
                return item.table == $scope.availableTables[i];
            });

            if(!tableInUse){
                modelsManager.selectedModel.dimensions = modelsManager.selectedModel.dimensions==null?[]:modelsManager.selectedModel.dimensions;
                modelsManager.selectedModel.dimensions.push(new Dimension($scope.availableTables[i]));
            }
        }

    };

    var Dimension = function(table){
        this.table = table;
        this.columns = [];
    }


  // Initialize data for columns widget in auto-gen when add/edit cube.
    if ($scope.state.mode == 'edit') {
        $scope.initColumns();
    };
});
