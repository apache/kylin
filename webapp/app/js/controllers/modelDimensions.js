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

KylinApp.controller('ModelDimensionsCtrl', function ($scope, $modal,MetaModel) {

    // Available columns list derived from cube data model.
    $scope.availableColumns = {};

    // Columns selected and disabled status bound to UI, group by table.
    $scope.selectedColumns = {};

    // Available tables cache: 1st is the fact table, next are lookup tables.
    $scope.availableTables = [];
    $scope.dimensions=[];

    // Dump available columns plus column table name, whether is from lookup table.
    $scope.initColumns = function () {
        var factTable = $scope.model.fact_table;

        $scope.availableTables.push($scope.model.fact_table);
        var lookups = $scope.model.lookups;
        for (var j = 0; j < lookups.length; j++) {
            $scope.availableTables.push(lookups[j].table);
        }

//        init dimension only when dimen
//        if(!$scope.model.dimensions.length){

            for(var i = 0;i<$scope.availableTables.length;i++){
                var tableInUse = _.some($scope.model.dimensions,function(item){
                    return item.table == $scope.availableTables[i];
                });

                if(!tableInUse){
                    $scope.model.dimensions.push(new Dimension($scope.availableTables[i]));
                }
            }
//        }

        // At first dump the columns of fact table.
//        var cols = $scope.getColumnsByTable(factTable);


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
