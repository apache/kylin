'use strict';


KylinApp.controller('CubeEditCtrl', function ($scope, $q, $routeParams, $location, $templateCache, $interpolate, MessageService, TableService, CubeDescService, CubeService, loadingRequest, SweetAlert,$log) {

    //add or edit ?
    var absUrl = $location.absUrl();
    $scope.cubeMode = absUrl.indexOf("/cubes/add")!=-1?'addNewCube':absUrl.indexOf("/cubes/edit")!=-1?'editExistCube':'default';

    //~ Define metadata & class
    $scope.measureParamType = ['column', 'constant'];
    $scope.measureExpressions = ['SUM', 'MIN', 'MAX', 'COUNT', 'COUNT_DISTINCT'];
    $scope.dimensionDataTypes = ["string", "tinyint", "int", "bigint", "date"];
    $scope.cubeCapacities = ["MEDIUM", "SMALL", "LARGE"];
    $scope.cubePartitionTypes = ['APPEND', 'UPDATE_INSERT'];
    $scope.joinTypes = [
        {name: 'Left', value: 'left'},
        {name: 'Inner', value: 'inner'},
        {name: 'Right', value: 'right'}
    ];
    $scope.queryPriorities = [
        {name: 'NORMAL', value: 50},
        {name: 'LOW', value: 70},
        {name: 'HIGH', value: 30}
    ];
    $scope.measureDataTypes = [
        {name: 'INT', value: 'int'},
        {name: 'BIGINT', value: 'bigint'},
        {name: 'DECIMAL', value: 'decimal'},
        {name: 'DOUBLE', value: 'double'},
        {name: 'DATE', value: 'date'},
        {name: 'STRING', value: 'string'}
    ];
    $scope.distinctDataTypes = [
        {name: 'Error Rate < 9.75%', value: 'hllc10'},
        {name: 'Error Rate < 4.88%', value: 'hllc12'},
        {name: 'Error Rate < 2.44%', value: 'hllc14'},
        {name: 'Error Rate < 1.72%', value: 'hllc15'},
        {name: 'Error Rate < 1.22%', value: 'hllc16'}
    ];

    $scope.dftSelections = {
        measureExpression: 'SUM',
        measureParamType: 'column',
        measureDataType: {name: 'BIGINT', value: 'bigint'},
        distinctDataType: {name: 'Error Rate < 2.44%', value: 'hllc14'},
        cubeCapacity: 'MEDIUM',
        queryPriority: {name: 'NORMAL', value: 50},
        cubePartitionType: 'APPEND'
    };

    $scope.dictionaries = ["true", "false"];
    $scope.srcTablesInProject = [];

    $scope.getColumnsByTable = function (name) {
        var temp = [];
        angular.forEach($scope.srcTablesInProject, function (table) {
            if (table.name == name) {
                temp = table.columns;
            }
        });
        return temp;
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

    var CubeMeta = {
        createNew: function () {
            var cubeMeta = {
                "name": "",
                "description": "",
                "fact_table": "",
                "filter_condition": null,
                "notify_list": [],
                "cube_partition_desc": {
                    "partition_date_column": null,
                    "partition_date_start": null,
                    "cube_partition_type": 'APPEND'
                },
                "capacity": "MEDIUM",
                "cost": 50,
                "dimensions": [],
                "measures": [
                    {   "id": 1,
                        "name": "_COUNT_",
                        "function": {
                            "expression": "COUNT",
                            "returntype": "bigint",
                            "parameter": {
                                "type": "constant",
                                "value": "1"
                            }
                        }
                    }
                ],
                "rowkey": {
                    "rowkey_columns": [],
                    "aggregation_groups": []
                },
                "hbase_mapping": {
                    "column_family": []
                }
            };

            return cubeMeta;
        }
    };

    // ~ Define data
    $scope.state = {
        "cubeSchema": ""
    };

    // ~ init
    if ($scope.isEdit = !!$routeParams.cubeName) {
        CubeDescService.get({cube_name: $routeParams.cubeName}, function (detail) {
            if (detail.length > 0) {
                $scope.cubeMetaFrame = detail[0];
                $scope.state.cubeSchema = angular.toJson($scope.cubeMetaFrame, true);
            }
        });
    } else {
        $scope.cubeMetaFrame = CubeMeta.createNew();
        $scope.cubeMetaFrame.project = $scope.project.selectedProject;
        $scope.state.cubeSchema = angular.toJson($scope.cubeMetaFrame, true);
    }

    // ~ public methods
    $scope.aceChanged = function () {
    };

    $scope.aceLoaded = function(){
    };

    $scope.prepareCube = function () {
        // generate column family
        generateColumnFamily();

        // Clean up objects used in cube creation
        angular.forEach($scope.cubeMetaFrame.dimensions, function (dimension, index) {
            // adpt join
            angular.forEach($scope.cubeMetaFrame.model.lookups, function (lookup, index) {
                if(lookup.table==dimension.table){
                    dimension.join = lookup.join;
                }
            });

            delete dimension.status;

            for (var key in dimension) {
                if (dimension.hasOwnProperty(key) && !dimension[key]) {
                    delete dimension[key];
                }
            }
        });


        if ($scope.cubeMetaFrame.cube_partition_desc.partition_date_start) {
            var dateStart = new Date($scope.cubeMetaFrame.cube_partition_desc.partition_date_start);
            dateStart = (dateStart.getFullYear() + "-" + (dateStart.getMonth() + 1) + "-" + dateStart.getDate());
            //switch selected time to utc timestamp
            $scope.cubeMetaFrame.cube_partition_desc.partition_date_start = new Date(moment.utc(dateStart, "YYYY-MM-DD").format()).getTime();
        }

        $scope.state.project = $scope.cubeMetaFrame.project;
//        delete $scope.cubeMetaFrame.project;

        $scope.state.cubeSchema = angular.toJson($scope.cubeMetaFrame, true);
    };

    $scope.cubeResultTmpl = function (notification) {
        // Get the static notification template.
        var tmpl = notification.type == 'success' ? 'cubeResultSuccess.html' : 'cubeResultError.html';
        return $interpolate($templateCache.get(tmpl))(notification);
    };

    $scope.saveCube = function (design_form) {

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
                            MessageService.sendMsg($scope.cubeResultTmpl({'text':'Updated the cube successfully.',type:'success'}), 'success', {}, true, 'top_center');

                            if (design_form) {
                                design_form.$invalid = true;
                            }
                        } else {
                            $scope.cubeMetaFrame.project = $scope.state.project;
                                var message =request.message;
                                var msg = !!(message) ? message : 'Failed to take action.';
                                MessageService.sendMsg($scope.cubeResultTmpl({'text':msg,'schema':$scope.state.cubeSchema}), 'error', {}, true, 'top_center');
                        }
                        //end loading
                        loadingRequest.hide();
                    }, function (e) {
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

                            MessageService.sendMsg($scope.cubeResultTmpl({'text':'Created the cube successfully.',type:'success'}), 'success', {}, true, 'top_center');
                        } else {
                            $scope.cubeMetaFrame.project = $scope.state.project;
                            var message =request.message;
                            var msg = !!(message) ? message : 'Failed to take action.';
                            MessageService.sendMsg($scope.cubeResultTmpl({'text':msg,'schema':$scope.state.cubeSchema}), 'error', {}, true, 'top_center');
                        }

                        //end loading
                        loadingRequest.hide();
                    }, function (e) {
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
        });
    };


    function reGenerateRowKey(){
        $log.log("reGen rowkey & agg group");
        var tmpRowKeyColumns = [];
        var tmpAggregationItems = [];
        var hierarchyItems = [];
        angular.forEach($scope.cubeMetaFrame.dimensions, function (dimension, index) {
            if (dimension.column == '{FK}' && dimension.join && dimension.join.foreign_key.length > 0) {
                angular.forEach(dimension.join.foreign_key, function (fk, index) {

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
                });
            }
            else if (dimension.column) {
                for (var i = 0; i < tmpRowKeyColumns.length; i++) {
                    if(tmpRowKeyColumns[i].column == dimension.column)
                        break;
                }
                if(i == tmpRowKeyColumns.length) {
                    tmpRowKeyColumns.push({
                        "column": dimension.column,
                        "length": 0,
                        "dictionary": "true",
                        "mandatory": false
                    });
                    tmpAggregationItems.push(dimension.column);
                }
            }
            if (dimension.hierarchy && dimension.hierarchy.length > 0) {
                angular.forEach(dimension.hierarchy, function (hierarchy, index) {
                    for (var i = 0; i < tmpRowKeyColumns.length; i++) {
                        if(tmpRowKeyColumns[i].column == hierarchy.column)
                            break;
                    }
                    if(i == tmpRowKeyColumns.length) {
                        tmpRowKeyColumns.push({
                            "column": hierarchy.column,
                            "length": 0,
                            "dictionary": "true",
                            "mandatory": false
                        });
                    tmpAggregationItems.push(hierarchy.column);
                    }
                    if(hierarchyItems.indexOf(hierarchy.column)==-1){
                        hierarchyItems.push(hierarchy.column);
                    }
                });
            }

        });

        var rowkeyColumns = $scope.cubeMetaFrame.rowkey.rowkey_columns;
        var newRowKeyColumns = sortSharedData(rowkeyColumns,tmpRowKeyColumns);
        var increasedColumns = increasedColumn(rowkeyColumns,tmpRowKeyColumns);
        newRowKeyColumns = newRowKeyColumns.concat(increasedColumns);

        //! here get the latest rowkey_columns
        $scope.cubeMetaFrame.rowkey.rowkey_columns = newRowKeyColumns;

        if($scope.cubeMode==="editExistCube") {
            var aggregationGroups = $scope.cubeMetaFrame.rowkey.aggregation_groups;
            // rm unused item from group
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

            var newUniqAggregationItem = [];
            angular.forEach(tmpAggregationItems, function (item, index) {
                if(newUniqAggregationItem.indexOf(item)==-1){
                    newUniqAggregationItem.push(item);
                }
            });

            var unHierarchyItems = increasedData(hierarchyItems,newUniqAggregationItem);
//            hierarchyItems
            var increasedDataGroups = sliceGroupItemToGroups(unHierarchyItems);
            if(hierarchyItems.length){
                increasedDataGroups.push(hierarchyItems);
            }

            //! here get the latest aggregation groups,only effect when add newCube
            $scope.cubeMetaFrame.rowkey.aggregation_groups = increasedDataGroups;
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
//        if(groups[groups.length-1].length<10){
//            groups.pop();
//        }
        return groups;
    }


    // ~ private methods
    function generateColumnFamily() {
        $scope.cubeMetaFrame.hbase_mapping.column_family = [];
        var colFamily = ColFamily();
        var normalMeasures = [], distinctCountMeasures=[];
        angular.forEach($scope.cubeMetaFrame.measures, function (measure, index) {
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
            $scope.cubeMetaFrame.hbase_mapping.column_family.push(nmcf);
        }

        if (distinctCountMeasures.length > 0){
            var dccf = colFamily();
            angular.forEach(distinctCountMeasures, function(dcm, index){
                dccf.columns[0].measure_refs.push(dcm.name);
            });
            $scope.cubeMetaFrame.hbase_mapping.column_family.push(dccf);
        }
    }


    $scope.$watch('project.selectedProject', function (newValue, oldValue) {
        if(!newValue){
            return;
        }
        $scope.srcTablesInProject=[];
        var param = {
            ext: true,
            project:newValue
        };
        if(newValue){
            TableService.list(param, function (tables) {
                angular.forEach(tables, function (table) {
                    $scope.srcTablesInProject.push(table);
                });
            });
        }
    });

    $scope.$on('DimensionsEdited', function (event) {
        if ($scope.cubeMetaFrame) {
            reGenerateRowKey();
        }
    });
});
