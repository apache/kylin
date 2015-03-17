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

KylinApp
    .controller('ProjectMetaCtrl', function ($scope, $q, ProjectService, QueryService) {
        $scope.selectedSrcDb = [];
        $scope.selectedSrcTable = {};
        $scope.treeOptions = {
            nodeChildren: "columns",
            injectClasses: {
                ul: "a1",
                li: "a2",
                liSelected: "a7",
                iExpanded: "a3",
                iCollapsed: "a4",
                iLeaf: "a5",
                label: "a6",
                labelSelected: "a8"
            }
        };

        $scope.showSelected = function (table) {
            if (table.uuid) {
                $scope.selectedSrcTable = table;
            }
            else {
                $scope.selectedSrcTable.selectedSrcColumn = table;
            }
        }

        $scope.projectMetaLoad = function () {
            var defer = $q.defer();
            $scope.selectedSrcDb = [];
            $scope.loading = true;
            QueryService.getTables({project: $scope.projectModel.selectedProject}, {}, function (tables) {
                var tableMap = [];
                angular.forEach(tables, function (table) {
                    if (!tableMap[table.table_SCHEM]) {
                        tableMap[table.table_SCHEM] = [];
                    }
                    table.name = table.table_NAME;
                    angular.forEach(table.columns, function (column, index) {
                        column.name = column.column_NAME;
                    });
                    tableMap[table.table_SCHEM].push(table);
                });

                for (var key in  tableMap) {
                    var tables = tableMap[key];
                    $scope.selectedSrcDb.push({
                        "name": key,
                        "columns": tables
                    });
                }

                $scope.loading = false;
                defer.resolve();
            });
            return defer.promise;
        };


        $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
                $scope.projectMetaLoad();
        });

        $scope.trimType = function(typeName){
            if (typeName.match(/VARCHAR/i))
            {
                typeName = "VARCHAR";
            }

            return  typeName.trim().toLowerCase();
        }

    });

