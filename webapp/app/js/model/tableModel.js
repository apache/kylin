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

KylinApp.service('TableModel', function(ProjectModel,$q,TableService,$log) {


    var _this = this;
   //for tables in cubeDesigner
    this.selectProjectTables = [];

    this.initTables = function(){
        this.selectProjectTables = [];
    }

    this.addTable = function(table){
        this.selectProjectTables.push(table);
    }

    this.setSelectedProjectTables = function(tables) {
        this.selectProjectTables = tables;
    }


  // for load table page
    this.selectedSrcDb = [];
    this.selectedSrcTable = {};

    this.init = function(){
      this.selectedSrcDb = [];
      this.selectedSrcTable = {};
    };

    this.aceSrcTbLoaded = function (forceLoad) {
        _this.selectedSrcDb = [];

        _this.selectedSrcTable = {};
        var defer = $q.defer();

        var param = {
            ext: true,
            project:ProjectModel.selectedProject
        };

        if(!ProjectModel.selectedProject){
            defer.resolve();
            return defer.promise;
        }

        TableService.list(param, function (tables) {
            var tableMap = [];
            angular.forEach(tables, function (table) {

                var tableData = [];

                if (!tableMap[table.database]) {
                    tableMap[table.database] = [];
                }
                angular.forEach(table.columns, function (column) {
                    if(table.cardinality[column.name]) {
                        column.cardinality = table.cardinality[column.name];
                    }else{
                        column.cardinality = null;
                    }
                    column.id = parseInt(column.id);
                });
                tableMap[table.database].push(table);
            });

//                Sort Table
            for (var key in  tableMap) {
                var obj = tableMap[key];
                obj.sort(_this.innerSort);
            }

            _this.selectedSrcDb = [];
            for (var key in  tableMap) {
                var tables = tableMap[key];

                var _db_node = {
                    label:key,
                    data:tables,
                    onSelect:function(branch){
                        $log.info("db "+key +"selected");
                    }
                }


                angular.forEach(tables,function(_table){
                        var _table_node_list = [];

                        var _table_node = {
                            label:_table.name,
//                            noLeaf:true,
                            data:_table,
                            onSelect:function(branch){
                                // set selected model
                                _this.selectedSrcTable = branch.data;
                            }
                        }

                        var _column_node_list = [];
                        angular.forEach(_table.columns,function(_column){
                            _column_node_list.push({
                                    label:_column.name,
                                    noLeaf:true,
                                    data:_column,
                                    onSelect:function(branch){
                                        // set selected model
//                                        _this.selectedSrcTable = branch.data;
                                        $log.info("selected column info:"+_column.name);
                                    }
                                });
                        });
                         _table_node.children =_column_node_list;
                        _table_node_list.push(_table_node);

                        _db_node.children = _table_node_list;
                    }
                );

                _this.selectedSrcDb.push(_db_node);
            }
            defer.resolve();
        });

        return defer.promise;
    };
    this.innerSort =function(a, b) {
        var nameA = a.name.toLowerCase(), nameB = b.name.toLowerCase();
        if (nameA < nameB) //sort string ascending
            return -1;
        if (nameA > nameB)
            return 1;
        return 0; //default return value (no sorting)
    };

});

