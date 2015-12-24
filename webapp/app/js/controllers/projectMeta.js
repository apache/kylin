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
    .controller('ProjectMetaCtrl', function ($scope, $q, ProjectService, QueryService,$log) {
        $scope.selectedSrcDb = [];
        $scope.selectedSrcTable = {};

        $scope.showSelected = function (table) {
            if (table.uuid) {
                $scope.selectedSrcTable = table;
            }
            else {
                $scope.selectedSrcTable.selectedSrcColumn = table;
            }
        }

      $scope.doubleClick = function (branch) {
        if(!branch.parent_uid){
          return;
        }

        var selectTable = false;
        if(branch.data&&branch.data.table_TYPE=="TABLE"){
          selectTable = true;
        }

        if(angular.isUndefined($scope.$parent.queryString)){
          $scope.$parent.queryString='';
        }
        if(selectTable)
          $scope.$parent.queryString += (branch.data.table_NAME+' ');
        else
          $scope.$parent.queryString += (branch.data.table_NAME+'.'+branch.data.column_NAME + ' ');


      }

        $scope.projectMetaLoad = function () {
            var defer = $q.defer();
            $scope.selectedSrcDb = [];
            if(!$scope.projectModel.getSelectedProject()) {
              return;
            }
            $scope.loading = true;
            QueryService.getTables({project: $scope.projectModel.getSelectedProject()}, {}, function (tables) {
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
                    var _db_node = {
                        label:key,
                        data:tables,
                        onSelect:function(branch){
                            $log.info("db "+key +"selected");
                        }
                    }

                    var _table_node_list = [];
                    angular.forEach(tables,function(_table){
                            var _table_node = {
                                label:_table.name,
                                data:_table,
                                icon:"fa fa-table",
                                onSelect:function(branch){
                                    // set selected model
                                    $scope.selectedSrcTable = branch.data;
                                }
                            }

                            var _column_node_list = [];
                            angular.forEach(_table.columns,function(_column){
                                _column_node_list.push({
                                    label:_column.name+$scope.columnTypeFormat(_column.type_NAME),
                                    data:_column,
                                    onSelect:function(branch){
                                        // set selected model
                                        $log.info("selected column info:"+_column.name);
                                    }
                                });
                            });
                            _table_node.children =_column_node_list;
                            _table_node_list.push(_table_node);

                            _db_node.children = _table_node_list;
                        }
                    );

                    $scope.selectedSrcDb.push(_db_node);
                }

                $scope.loading = false;
                defer.resolve();
            });
            return defer.promise;
        };


        $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
                $scope.projectMetaLoad();
        });

        $scope.columnTypeFormat = function(typeName){
            if(typeName){
                return "("+$scope.trimType(typeName)+")";
            }else{
                return "";
            }
        }
        $scope.trimType = function(typeName){
            if (typeName.match(/VARCHAR/i))
            {
                typeName = "VARCHAR";
            }

            return  typeName.trim().toLowerCase();
        }

    });

