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

KylinApp.service('TableModel', function(ProjectModel,$q,TableService,$log,EncodingService,tableConfig) {


    var _this = this;

    //tracking loading status
    var loading = false;
   //for tables in cubeDesigner
    this.selectProjectTables = [];
    this.columnNameTypeMap = {};
    this.columnTypeEncodingMap={};

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
    this.getcolumnNameTypeMap=function(callback){
      var param = {
        ext: true,
        project:ProjectModel.selectedProject
      };
      if(angular.equals({}, _this.columnNameTypeMap)) {
        TableService.list(param, function (tables) {

          angular.forEach(tables, function (table) {
            angular.forEach(table.columns, function (column) {
              var tableName=table.database+"."+table.name;
              _this.columnNameTypeMap[tableName+'.'+column.name] = column.datatype;
            });
          });
          if(typeof  callback=='function'){
            callback(_this.columnNameTypeMap);
          }
        });
      }else{
        if(typeof  callback=='function'){
          callback(_this.columnNameTypeMap);
        }
      }
    }
    this.aceSrcTbLoaded = function (forceLoad) {
        _this.selectedSrcDb = [];
        _this.loading = true;

        _this.selectedSrcTable = {};
        var defer = $q.defer();

        var param = {
            ext: true,
            project:ProjectModel.selectedProject
        };

        if(!ProjectModel.selectedProject||!ProjectModel.isSelectedProjectValid()){
            defer.resolve();
            return defer.promise;
        }

      TableService.list(param, function (tables) {
            var tableMap = [];
            angular.forEach(tables, function (table) {
              var tableName=table.database+"."+table.name;
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
                  _this.columnNameTypeMap[tableName+'.'+column.name] = column.datatype;
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

                var _table_node_list = [];
                angular.forEach(tables,function(_table){

                        var table_icon="fa fa-table";
                        if(_.values(tableConfig.streamingSourceType).indexOf(_table.source_type) > -1 || _table.source_type==1){
                          table_icon="fa fa-th"
                        }

                        var _table_node = {
                            label:_table.name,
                            data:_table,
                            icon:table_icon,
                            onSelect:function(branch){
                                // set selected model
                                _this.selectedSrcTable = branch.data;
                            }
                        }

                        var _column_node_list = [];
                        angular.forEach(_table.columns,function(_column){
                            _column_node_list.push({
                                    label:_column.name+"("+_column.datatype+")",
                                    data:_column,
                                    onSelect:function(branch){
                                        // set selected model
//                                        _this.selectedSrcTable = branch.data;
                                        _this.selectedSrcTable.selectedSrcColumn = branch.data;
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
            _this.loading = false;
            defer.resolve();
        },function(e){
          defer.reject("Failed to load tables, please check system log for details.");
        });

        return defer.promise;
    };
    this.getColumnTypeEncodingMap=function(){
      var _this=this;
      var defer = $q.defer();
      if(!angular.equals({},_this.columnTypeEncodingMap)){
        defer.resolve(_this.columnTypeEncodingMap);
      }
      EncodingService.getEncodingMap({},{},function(result){
        if(result&&result.data){
          _this.columnTypeEncodingMap=result.data;
        }else{
          _this.columnTypeEncodingMap=tableConfig.columnTypeEncodingMap;
        }
        defer.resolve(_this.columnTypeEncodingMap);
      },function(){
        _this.columnTypeEncodingMap=tableConfig.columnTypeEncodingMap;
        defer.resolve(_this.columnTypeEncodingMap);
      })

      return defer.promise;
    }
    this.getColumnType = function(_column,_table){
        var columns = _this.getColumnsByTable(_table);
        var type;
        angular.forEach(columns,function(column){
            if(_column === column.name){
                type = column.datatype;
                return;
            }
        });
        return type;
    };

    this.getColumnsByTable = function (tableName) {
        var temp = [];
        angular.forEach(_this.selectProjectTables, function (table) {
            if (table.name == tableName) {
                temp = table.columns;
            }
        });
        return temp;
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

