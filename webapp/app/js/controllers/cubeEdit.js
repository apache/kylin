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


KylinApp.controller('CubeEditCtrl', function ($scope, $q, $routeParams, $location, $templateCache, $interpolate, MessageService, TableService, CubeDescService, CubeService, loadingRequest, SweetAlert,$log,cubeConfig,CubeDescModel,MetaModel,TableModel,ModelDescService,modelsManager,cubesManager) {
    $scope.cubeConfig = cubeConfig;

    // when add cube will transfer model Name
    var modelName = $routeParams.modelName;

    //add or edit ?
    var absUrl = $location.absUrl();
    $scope.cubeMode = absUrl.indexOf("/cubes/add")!=-1?'addNewCube':absUrl.indexOf("/cubes/edit")!=-1?'editExistCube':'default';


    $scope.getColumnsByTable = function (tableName) {
        var temp = [];
        angular.forEach(TableModel.selectProjectTables, function (table) {
            if (table.name == tableName) {
                temp = table.columns;
            }
        });
        return temp;
    };

    $scope.getDimColumnsByTable = function (tableName) {
        if(!tableName){
            return [];
        }
        var tableColumns = $scope.getColumnsByTable(tableName);
        var tableDim = _.find($scope.metaModel.model.dimensions,function(dimension){return dimension.table == tableName});
        var tableDimColumns = tableDim.columns;
        var avaColObject = _.filter(tableColumns,function(col){
            return tableDimColumns.indexOf(col.name)!=-1;
        });
        return avaColObject;
    };

    $scope.getMetricColumnsByTable = function (tableName) {
        if(!tableName){
            return [];
        }
        var tableColumns = $scope.getColumnsByTable(tableName);
        var tableMetrics = $scope.metaModel.model.metrics;
        var avaColObject = _.filter(tableColumns,function(col){
            return tableMetrics.indexOf(col.name)!=-1;
        });
        return avaColObject;
    };

    $scope.getPartitonColumns = function(tableName){
        var columns = _.filter($scope.getColumnsByTable(tableName),function(column){
            return column.datatype==="date"||column.datatype==="string";
        });
        return columns;
    };

    $scope.getColumnType = function (_column,table){
        var columns = $scope.getColumnsByTable(table);
        var type;
        angular.forEach(columns,function(column){
            if(_column === column.name){
                type = column.datatype;
                return;
            }
        });
        return type;
    };

    var ColFamily = function () {
        var index = 1;
        return function () {
            var newColFamily =
            {
                "name": "f" + index,
                "columns": [
                    {
                        "qualifier": "m",
                        "measure_refs": []
                    }
                ]
            };
            index += 1;

            return  newColFamily;
        }
    };


    // ~ Define data
    $scope.state = {
        "cubeSchema": "",
        "mode":'edit'
    };

    //fetch cube info and model info in edit model
    // ~ init
    if ($scope.isEdit = !!$routeParams.cubeName) {
        CubeDescService.get({cube_name: $routeParams.cubeName}, function (detail) {
            if (detail.length > 0) {
                cubesManager.cubeMetaFrame = detail[0];
                cubesManager.cubeMetaFrame = detail[0];
                $scope.metaModel = {};

                //get model from API when page refresh
                if(!modelsManager.getModels().length){
                    ModelDescService.get({model_name: cubesManager.cubeMetaFrame.model_name}, function (_model) {
                        $scope.metaModel.model = _model;
                    });
                }
                $scope.metaModel.model=modelsManager.getModel(cubesManager.cubeMetaFrame.model_name);

                $scope.state.cubeSchema = angular.toJson(cubesManager.cubeMetaFrame, true);
            }
        });

    } else {
//        cubesManager.cubeMetaFrame = CubeDescModel.createNew();
        cubesManager.cubeMetaFrame = CubeDescModel.createNew();
        $scope.metaModel ={
            model : modelsManager.getModel(modelName)
        }
        cubesManager.cubeMetaFrame.model_name = modelName;
        $scope.state.cubeSchema = angular.toJson(cubesManager.cubeMetaFrame, true);
    }


    $scope.prepareCube = function () {
        // generate column family
        generateColumnFamily();
        //generate rowkey TODO remove after refactor
        reGenerateRowKey();


        if ($scope.metaModel.model.partition_desc.partition_date_column&&($scope.metaModel.model.partition_desc.partition_date_start|$scope.metaModel.model.partition_desc.partition_date_start==0)) {
            var dateStart = new Date($scope.metaModel.model.partition_desc.partition_date_start);
            dateStart = (dateStart.getFullYear() + "-" + (dateStart.getMonth() + 1) + "-" + dateStart.getDate());
            //switch selected time to utc timestamp
            $scope.metaModel.model.partition_desc.partition_date_start = new Date(moment.utc(dateStart, "YYYY-MM-DD").format()).getTime();


            if($scope.metaModel.model.partition_desc.partition_date_column.indexOf(".")==-1){
            $scope.metaModel.model.partition_desc.partition_date_column=$scope.metaModel.model.fact_table+"."+$scope.metaModel.model.partition_desc.partition_date_column;
            }

        }
        //use cubedesc name as model name
        if($scope.metaModel.model.name===""||angular.isUndefined($scope.metaModel.model.name)){
            $scope.metaModel.model.name = cubesManager.cubeMetaFrame.name;
        }

        //set model ref for cubeDesc
        if(cubesManager.cubeMetaFrame.model_name===""||angular.isUndefined(cubesManager.cubeMetaFrame.model_name)){
            cubesManager.cubeMetaFrame.model_name = cubesManager.cubeMetaFrame.name;
        }

        $scope.state.project = cubesManager.cubeMetaFrame.project;
//        delete cubesManager.cubeMetaFrame.project;

        $scope.state.cubeSchema = angular.toJson(cubesManager.cubeMetaFrame, true);
    };

    $scope.cubeResultTmpl = function (notification) {
        // Get the static notification template.
        var tmpl = notification.type == 'success' ? 'cubeResultSuccess.html' : 'cubeResultError.html';
        return $interpolate($templateCache.get(tmpl))(notification);
    };

    $scope.saveCube = function () {

        try {
            angular.fromJson($scope.state.cubeSchema);
        } catch (e) {
            SweetAlert.swal('Oops...', 'Invalid cube json format..', 'error');
            return;
        }

        SweetAlert.swal({
            title: '',
            text: 'Are you sure to save the cube ?',
            type: '',
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            confirmButtonText: "Yes",
            closeOnConfirm: true
        }, function(isConfirm) {
            if(isConfirm){
                loadingRequest.show();

                if ($scope.isEdit) {
                    CubeService.update({}, {cubeDescData: $scope.state.cubeSchema, cubeName: $routeParams.cubeName, project: $scope.state.project}, function (request) {
                        if (request.successful) {
                            $scope.state.cubeSchema = request.cubeDescData;
                            SweetAlert.swal('', 'Updated the cube successfully.', 'success');
                            $location.path("/models");
                        } else {
                            $scope.saveCubeRollBack();
                            cubesManager.cubeMetaFrame.project = $scope.state.project;
                                var message =request.message;
                                var msg = !!(message) ? message : 'Failed to take action.';
                                MessageService.sendMsg($scope.cubeResultTmpl({'text':msg,'schema':$scope.state.cubeSchema}), 'error', {}, true, 'top_center');
                        }
                        //end loading
                        loadingRequest.hide();
                    }, function (e) {
                        $scope.saveCubeRollBack();

                        if(e.data&& e.data.exception){
                            var message =e.data.exception;
                            var msg = !!(message) ? message : 'Failed to take action.';
                            MessageService.sendMsg($scope.cubeResultTmpl({'text':msg,'schema':$scope.state.cubeSchema}), 'error', {}, true, 'top_center');
                        } else {
                            MessageService.sendMsg($scope.cubeResultTmpl({'text':'Failed to take action.','schema':$scope.state.cubeSchema}), 'error', {}, true, 'top_center');
                        }
                        loadingRequest.hide();
                    });
                } else {
                    CubeService.save({}, {cubeDescData: $scope.state.cubeSchema, project: $scope.state.project}, function (request) {
                        if(request.successful) {
                            $scope.state.cubeSchema = request.cubeDescData;

                            SweetAlert.swal('', 'Created the cube successfully.', 'success');
                            $location.path("/models");
                            //location.reload();

                        } else {
                            $scope.saveCubeRollBack();
                            cubesManager.cubeMetaFrame.project = $scope.state.project;
                            var message =request.message;
                            var msg = !!(message) ? message : 'Failed to take action.';
                            MessageService.sendMsg($scope.cubeResultTmpl({'text':msg,'schema':$scope.state.cubeSchema}), 'error', {}, true, 'top_center');
                        }

                        //end loading
                        loadingRequest.hide();
                    }, function (e) {
                        $scope.saveCubeRollBack();

                        if (e.data && e.data.exception) {
                            var message =e.data.exception;
                            var msg = !!(message) ? message : 'Failed to take action.';
                            MessageService.sendMsg($scope.cubeResultTmpl({'text':msg,'schema':$scope.state.cubeSchema}), 'error', {}, true, 'top_center');
                        } else {
                            MessageService.sendMsg($scope.cubeResultTmpl({'text':"Failed to take action.",'schema':$scope.state.cubeSchema}), 'error', {}, true, 'top_center');
                        }
                        //end loading
                        loadingRequest.hide();

                    });
                }
            }
            else{
                $scope.saveCubeRollBack();
            }
        });
    };

//    reverse the date
    $scope.saveCubeRollBack = function (){
        if($scope.metaModel.model&&($scope.metaModel.model.partition_desc.partition_date_start||$scope.metaModel.model.partition_desc.partition_date_start==0))
        {
            $scope.metaModel.model.partition_desc.partition_date_start+=new Date().getTimezoneOffset()*60000;
        }
    }

    $scope.updateMandatory = function(rowkey_column){
        if(!rowkey_column.mandatory){
            angular.forEach(cubesManager.cubeMetaFrame.rowkey.aggregation_groups, function (group, index) {
                   var index = group.indexOf(rowkey_column.column);
                   if(index>-1){
                       group.splice(index,1);
                   }
            });
        }
    }

    function reGenerateRowKey(){
        $log.log("reGen rowkey & agg group");
        var tmpRowKeyColumns = [];
        var tmpAggregationItems = [];//put all aggregation item
        var hierarchyItemArray = [];//put all hierarchy items
        angular.forEach(cubesManager.cubeMetaFrame.dimensions, function (dimension, index) {

           //derived column
            if(dimension.derived&&dimension.derived.length){
                var lookup = _.find($scope.metaModel.model.lookups,function(lookup){return lookup.table==dimension.table});
                angular.forEach(lookup.join.foreign_key, function (fk, index) {
                    for (var i = 0; i < tmpRowKeyColumns.length; i++) {
                        if(tmpRowKeyColumns[i].column == fk)
                            break;
                    }
                    // push to array if no duplicate value
                    if(i == tmpRowKeyColumns.length) {
                        tmpRowKeyColumns.push({
                            "column": fk,
                            "length": 0,
                            "dictionary": "true",
                            "mandatory": false
                        });

                        tmpAggregationItems.push(fk);
                    }
                })

            }
            //normal column
            else if (dimension.column&&!dimension.hierarchy&&dimension.column.length==1) {
                for (var i = 0; i < tmpRowKeyColumns.length; i++) {
                    if(tmpRowKeyColumns[i].column == dimension.column[0])
                        break;
                }
                if(i == tmpRowKeyColumns.length) {
                    tmpRowKeyColumns.push({
                        "column": dimension.column[0],
                        "length": 0,
                        "dictionary": "true",
                        "mandatory": false
                    });
                    tmpAggregationItems.push(dimension.column[0]);
                }
            }
            // hierarchy
            if(dimension.hierarchy && dimension.column.length){
                var hierarchyUnit = [];
                angular.forEach(dimension.column, function (hier_column, index) {
                    for (var i = 0; i < tmpRowKeyColumns.length; i++) {
                        if(tmpRowKeyColumns[i].column == hier_column)
                            break;
                    }
                    if(i == tmpRowKeyColumns.length) {
                        tmpRowKeyColumns.push({
                            "column": hier_column,
                            "length": 0,
                            "dictionary": "true",
                            "mandatory": false
                        });
                        tmpAggregationItems.push(hier_column);
                    }
                    if(hierarchyUnit.indexOf(hier_column)==-1){
                      hierarchyUnit.push(hier_column);
                    }
                });
              if(hierarchyUnit.length){
                hierarchyItemArray.push(hierarchyUnit);
              }
            }

        });


        //rm mandatory column from aggregation item
        angular.forEach(cubesManager.cubeMetaFrame.rowkey.rowkey_columns,function(value,index){
                if(value.mandatory){
                    tmpAggregationItems = _.filter(tmpAggregationItems,function(item){
                           return item!=value.column;
                    });
                }
        });

        var rowkeyColumns = cubesManager.cubeMetaFrame.rowkey.rowkey_columns;
        var newRowKeyColumns = sortSharedData(rowkeyColumns,tmpRowKeyColumns);
        var increasedColumns = increasedColumn(rowkeyColumns,tmpRowKeyColumns);
        newRowKeyColumns = newRowKeyColumns.concat(increasedColumns);

        //! here get the latest rowkey_columns
        cubesManager.cubeMetaFrame.rowkey.rowkey_columns = newRowKeyColumns;

        if($scope.cubeMode==="editExistCube") {
            var aggregationGroups = cubesManager.cubeMetaFrame.rowkey.aggregation_groups;
            // rm unused item from group,will only rm when [edit] dimension
            angular.forEach(aggregationGroups, function (group, index) {
                if (group) {
                    for (var j = 0; j < group.length; j++) {
                        var elemStillExist = false;
                        for (var k = 0; k < tmpAggregationItems.length; k++) {
                            if (group[j] == tmpAggregationItems[k]) {
                                elemStillExist = true;
                                break;
                            }
                        }
                        if (!elemStillExist) {
                            group.splice(j, 1);
                            j--;
                        }
                    }
                    if (!group.length) {
                        aggregationGroups.splice(index, 1);
                        index--;
                    }
                }
                else {
                    aggregationGroups.splice(index, 1);
                    index--;
                }
            });
        }

        if($scope.cubeMode==="addNewCube"){

          if(!tmpAggregationItems.length) {
              cubesManager.cubeMetaFrame.rowkey.aggregation_groups=[];
              return;
          }

            var newUniqAggregationItem = [];
            angular.forEach(tmpAggregationItems, function (item, index) {
                if(newUniqAggregationItem.indexOf(item)==-1){
                    newUniqAggregationItem.push(item);
                }
            });

          var hierarchyItems = hierarchyItemArray.join().split(",");
          var unHierarchyItems = increasedData(hierarchyItems,newUniqAggregationItem);
          //hierarchyItems
          var increasedDataGroups = sliceGroupItemToGroups(unHierarchyItems);
          if(!hierarchyItemArray.length){
              cubesManager.cubeMetaFrame.rowkey.aggregation_groups = increasedDataGroups;
              return;
          };

          var lastAggregationGroup = increasedDataGroups.length===0?[]:increasedDataGroups[increasedDataGroups.length-1];

          if(lastAggregationGroup.length<10){
            if(lastAggregationGroup.length+hierarchyItemArray.length<=10){
              lastAggregationGroup = lastAggregationGroup.concat(hierarchyItems);
              if(increasedDataGroups.length==0){
                //case only hierarchy
                increasedDataGroups[0]=lastAggregationGroup;
              }else{
                increasedDataGroups[increasedDataGroups.length-1]=lastAggregationGroup;
              }
            }
            else{
                var cutIndex = 10-lastAggregationGroup.length;
                var partialHierarchy =hierarchyItemArray.slice(0,cutIndex).join().split(",");
                //add hierarchy to last group and make sure length less than 10
                lastAggregationGroup = lastAggregationGroup.concat(partialHierarchy);
                increasedDataGroups[increasedDataGroups.length-1]=lastAggregationGroup;
                var leftHierarchy = hierarchyItemArray.slice(cutIndex);

                var leftHierarchyLength = leftHierarchy.length;
                var grpLength = parseInt(leftHierarchyLength/10);
                if(leftHierarchyLength%10==0&&leftHierarchyLength!=0){
                    grpLength--;
                }
                for(var i=0;i<=grpLength;i++){
                    var hierAggGroupUnit = leftHierarchy.slice(i*10,(i+1)*10).join().split(",");
                    increasedDataGroups.push(hierAggGroupUnit);
                }
            }
          }
          //lastAggregationGroup length >=10
          else{
              var hierrachyArrayLength = hierarchyItemArray.length;
              var grpLength = parseInt(hierrachyArrayLength/10);
              if(hierrachyArrayLength%10==0&&hierrachyArrayLength!=0){
                  grpLength--;
              }
              for(var i=0;i<=grpLength;i++){
                   var hierAggGroupUnit = hierarchyItemArray.slice(i*10,(i+1)*10).join().split(",");
                   increasedDataGroups.push(hierAggGroupUnit);
              }
          }
            //! here get the latest aggregation groups,only effect when add newCube
            cubesManager.cubeMetaFrame.rowkey.aggregation_groups = increasedDataGroups;
        }
    }

    function sortSharedData(oldArray,tmpArr){
        var newArr = [];
        for(var j=0;j<oldArray.length;j++){
            var unit = oldArray[j];
            for(var k=0;k<tmpArr.length;k++){
                if(unit.column==tmpArr[k].column){
                    newArr.push(unit);
                }
            }
        }
        return newArr;
    }

    function increasedData(oldArray,tmpArr){
        var increasedData = [];
        if(oldArray&&!oldArray.length){
            return   increasedData.concat(tmpArr);
        }

        for(var j=0;j<tmpArr.length;j++){
            var unit = tmpArr[j];
            var exist = false;
            for(var k=0;k<oldArray.length;k++){
                if(unit==oldArray[k]){
                    exist = true;
                    break;
                }
            }
            if(!exist){
                increasedData.push(unit);
            }
        }
        return increasedData;
    }

    function increasedColumn(oldArray,tmpArr){
        var increasedData = [];
        if(oldArray&&!oldArray.length){
         return   increasedData.concat(tmpArr);
        }

        for(var j=0;j<tmpArr.length;j++){
            var unit = tmpArr[j];
            var exist = false;
            for(var k=0;k<oldArray.length;k++){
                if(unit.column==oldArray[k].column){
                    exist = true;
                    break;
                }
            }
            if(!exist){
                increasedData.push(unit);
            }
        }
        return increasedData;
    }

    function sliceGroupItemToGroups(groupItems){
        if(!groupItems.length){
            return [];
        }
        var groups = [];
        var j = -1;
        for(var i = 0;i<groupItems.length;i++){
            if(i%10==0){
                j++;
                groups[j]=[];
            }
            groups[j].push(groupItems[i]);
        }
        return groups;
    }


    // ~ private methods
    function generateColumnFamily() {
        cubesManager.cubeMetaFrame.hbase_mapping.column_family = [];
        var colFamily = ColFamily();
        var normalMeasures = [], distinctCountMeasures=[];
        angular.forEach(cubesManager.cubeMetaFrame.measures, function (measure, index) {
            if(measure.function.expression === 'COUNT_DISTINCT'){
                distinctCountMeasures.push(measure);
            }else{
                normalMeasures.push(measure);
            }
        });
        if(normalMeasures.length>0){
            var nmcf = colFamily();
            angular.forEach(normalMeasures, function(normalM, index){
                nmcf.columns[0].measure_refs.push(normalM.name);
            });
            cubesManager.cubeMetaFrame.hbase_mapping.column_family.push(nmcf);
        }

        if (distinctCountMeasures.length > 0){
            var dccf = colFamily();
            angular.forEach(distinctCountMeasures, function(dcm, index){
                dccf.columns[0].measure_refs.push(dcm.name);
            });
            cubesManager.cubeMetaFrame.hbase_mapping.column_family.push(dccf);
        }
    }

    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
        if(!newValue){
            return;
        }
        var param = {
            ext: true,
            project:newValue
        };
        if(newValue){
            TableModel.initTables();
            TableService.list(param, function (tables) {
                angular.forEach(tables, function (table) {
                    table.name = table.database+"."+table.name;
                    TableModel.addTable(table);
                });
            });
        }
    });

    $scope.$on('DimensionsEdited', function (event) {
        if (cubesManager.cubeMetaFrame) {
            reGenerateRowKey();
        }
    });
});
