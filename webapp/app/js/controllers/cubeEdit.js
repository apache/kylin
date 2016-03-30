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


KylinApp.controller('CubeEditCtrl', function ($scope, $q, $routeParams, $location, $templateCache, $interpolate, MessageService, TableService, CubeDescService, CubeService, loadingRequest, SweetAlert, $log, cubeConfig, CubeDescModel, MetaModel, TableModel, ModelDescService, modelsManager, cubesManager, ProjectModel, StreamingModel, StreamingService) {
  var STREAMING_SUFFIX = "_streaming";
  $scope.cubeConfig = cubeConfig;

  $scope.metaModel = {};
  $scope.modelsManager = modelsManager;

  //add or edit ?
  var absUrl = $location.absUrl();
  $scope.cubeMode = absUrl.indexOf("/cubes/add") != -1 ? 'addNewCube' : absUrl.indexOf("/cubes/edit") != -1 ? 'editExistCube' : 'default';

  if ($scope.cubeMode == "addNewCube" &&ProjectModel.selectedProject==null) {
    SweetAlert.swal('Oops...', 'Please select your project first.', 'warning');
    $location.path("/models");
    return;
  }


  //init encoding list
  $scope.store = {
    supportedEncoding:[]
  }
  CubeService.getValidEncodings({}, function (encodings) {
    $scope.store.supportedEncoding = encodings;
  },function(e){
    $scope.store.supportedEncoding = $scope.cubeConfig.encodings;
  })

  $scope.getColumnsByTable = function (tableName) {
    var temp = [];
    angular.forEach(TableModel.selectProjectTables, function (table) {
      if (table.name == tableName) {
        temp = table.columns;
      }
    });
    return temp;
  };

  //get columns from model
  $scope.getDimColumnsByTable = function (tableName) {
    if (!tableName) {
      return [];
    }
    var tableColumns = $scope.getColumnsByTable(tableName);
    var tableDim = _.find($scope.metaModel.model.dimensions, function (dimension) {
      return dimension.table == tableName
    });
    if(!tableDim){
      return [];
    }
    var tableDimColumns = tableDim.columns;
    var avaColObject = _.filter(tableColumns, function (col) {
      return tableDimColumns.indexOf(col.name) != -1;
    });
    return avaColObject;
  };

  $scope.getMetricColumnsByTable = function (tableName) {
    if (!tableName) {
      return [];
    }
    var tableColumns = $scope.getColumnsByTable(tableName);
    var tableMetrics = $scope.metaModel.model.metrics;
    var avaColObject = _.filter(tableColumns, function (col) {
      return tableMetrics.indexOf(col.name) != -1;
    });
    return avaColObject;
  };

  //SUM,COUNT,MAX,MIN
  $scope.getCommonMetricColumns = function () {

    //metric from model
    var me_columns = [];
    if($scope.metaModel.model.metrics){
      angular.forEach($scope.metaModel.model.metrics,function(metric,index){
        me_columns.push(metric);
      })
    }
    return me_columns;
  };

  //COUNT_DISTINCT,TopN
  $scope.getMetricColumns = function () {

    //metric from model
    var me_columns = [];
    if($scope.metaModel.model.metrics){
      angular.forEach($scope.metaModel.model.metrics,function(metric,index){
        me_columns.push(metric);
      })
    }

    //add cube dimension column for specific measure
    angular.forEach($scope.cubeMetaFrame.dimensions,function(dimension,index){
      if(dimension.column && dimension.derived == null){
        me_columns.push(dimension.column);
      }
    });

    return me_columns;
  };


  $scope.getColumnType = function (_column, table) {
    var columns = $scope.getColumnsByTable(table);
    var type;
    angular.forEach(columns, function (column) {
      if (_column === column.name) {
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

      return newColFamily;
    }
  };


  // ~ Define data
  $scope.state = {
    "cubeSchema": "",
    "mode": 'edit'
  };

  $scope.cubeState={
    "isStreaming": false
  }

  //fetch cube info and model info in edit model
  // ~ init
  if ($scope.isEdit = !!$routeParams.cubeName) {

    $scope.streamingMeta = StreamingModel.createStreamingConfig();
    $scope.kafkaMeta = StreamingModel.createKafkaConfig();

    CubeDescService.query({cube_name: $routeParams.cubeName}, function (detail) {
      if (detail.length > 0) {
        $scope.cubeMetaFrame = detail[0];
        $scope.metaModel = {};

        //get model from API when page refresh
        if (!modelsManager.getModels().length) {
          ModelDescService.query({model_name: $scope.cubeMetaFrame.model_name}, function (_model) {
            $scope.metaModel.model = _model;

            StreamingService.getConfig({table:$scope.metaModel.model.fact_table}, function (kfkConfigs) {
              if(!!kfkConfigs[0]){
                $scope.cubeState.isStreaming = true;
              }
              else{
                return;
              }
              $scope.streamingMeta = kfkConfigs[0];
              StreamingService.getKfkConfig({kafkaConfigName:$scope.streamingMeta.name}, function (streamings) {
                $scope.kafkaMeta = streamings[0];
              })
            })
          });
        }

        $scope.state.cubeSchema = angular.toJson($scope.cubeMetaFrame, true);


      }
    });



  } else {
//        $scope.cubeMetaFrame = CubeDescModel.createNew();
    $scope.cubeMetaFrame = CubeDescModel.createNew();
    $scope.metaModel = {
      model: modelsManager.getModel($scope.cubeMetaFrame.model_name)
    }
    //$scope.cubeMetaFrame.model_name = modelName;
    $scope.state.cubeSchema = angular.toJson($scope.cubeMetaFrame, true);

    $scope.streamingMeta = StreamingModel.createStreamingConfig();
    $scope.kafkaMeta = StreamingModel.createKafkaConfig();

  }


  $scope.prepareCube = function () {
    //generate column family
    generateColumnFamily();
    //generate rowkey
    reGenerateRowKey();

    if ($scope.metaModel.model.partition_desc.partition_date_column && ($scope.cubeMetaFrame.partition_date_start | $scope.cubeMetaFrame.partition_date_start == 0)) {

      if ($scope.metaModel.model.partition_desc.partition_date_column.indexOf(".") == -1) {
        $scope.metaModel.model.partition_desc.partition_date_column = $scope.metaModel.model.fact_table + "." + $scope.metaModel.model.partition_desc.partition_date_column;
      }

    }

    //set model ref for cubeDesc
    if ($scope.cubeMetaFrame.model_name === "" || angular.isUndefined($scope.cubeMetaFrame.model_name)) {
      $scope.cubeMetaFrame.model_name = $scope.cubeMetaFrame.name;
    }

    $scope.state.project = ProjectModel.getSelectedProject();
//        delete $scope.cubeMetaFrame.project;


    $scope.state.cubeSchema = angular.toJson($scope.cubeMetaFrame, true);

    //streaming meta
    if($scope.cubeState.isStreaming == true){
      $scope.streamingMeta.cubeName = $scope.cubeMetaFrame.name;
      $scope.streamingMeta.name = $scope.cubeMetaFrame.name+STREAMING_SUFFIX;
      $scope.kafkaMeta.name = $scope.cubeMetaFrame.name+STREAMING_SUFFIX;
    }



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

    if (!$scope.cubeState.isStreaming) {
      $scope.state.streamingCube = false;
    } else {
      $scope.state.streamingCube = true;
      $scope.state.streamingMeta = angular.toJson($scope.streamingMeta, true);
      $scope.state.kafkaMeta = angular.toJson($scope.kafkaMeta, true);
    }


    SweetAlert.swal({
      title: '',
      text: 'Are you sure to save the cube ?',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {
        loadingRequest.show();

        if ($scope.isEdit) {
          CubeService.update({}, {
            cubeDescData: $scope.state.cubeSchema,
            cubeName: $routeParams.cubeName,
            project: $scope.state.project,
            streamingCube: $scope.state.streamingCube,
            streamingData: $scope.state.streamingMeta,
            kafkaData: $scope.state.kafkaMeta
          }, function (request) {
            if (request.successful) {
              $scope.state.cubeSchema = request.cubeDescData;
              SweetAlert.swal('', 'Updated the cube successfully.', 'success');
              $location.path("/models");
            } else {
              $scope.saveCubeRollBack();
              $scope.cubeMetaFrame.project = $scope.state.project;
              var message = request.message;
              var msg = !!(message) ? message : 'Failed to take action.';
              MessageService.sendMsg($scope.cubeResultTmpl({
                'text': msg,
                'schema': $scope.state.cubeSchema
              }), 'error', {}, true, 'top_center');
            }
            //end loading
            loadingRequest.hide();
          }, function (e) {
            $scope.saveCubeRollBack();

            if (e.data && e.data.exception) {
              var message = e.data.exception;
              var msg = !!(message) ? message : 'Failed to take action.';
              MessageService.sendMsg($scope.cubeResultTmpl({
                'text': msg,
                'schema': $scope.state.cubeSchema
              }), 'error', {}, true, 'top_center');
            } else {
              MessageService.sendMsg($scope.cubeResultTmpl({
                'text': 'Failed to take action.',
                'schema': $scope.state.cubeSchema
              }), 'error', {}, true, 'top_center');
            }
            loadingRequest.hide();
          });
        } else {
          CubeService.save({}, {
            cubeDescData: $scope.state.cubeSchema,
            project: $scope.state.project,
            streamingCube: $scope.state.streamingCube,
            streamingData: $scope.state.streamingMeta,
            kafkaData: $scope.state.kafkaMeta
          }, function (request) {
            if (request.successful) {
              $scope.state.cubeSchema = request.cubeDescData;
              SweetAlert.swal('', 'Created the cube successfully.', 'success');
              $location.path("/models");
              //location.reload();

            } else {
              $scope.saveCubeRollBack();
              $scope.cubeMetaFrame.project = $scope.state.project;
              var message = request.message;
              var msg = !!(message) ? message : 'Failed to take action.';
              MessageService.sendMsg($scope.cubeResultTmpl({
                'text': msg,
                'schema': $scope.state.cubeSchema
              }), 'error', {}, true, 'top_center');
            }

            //end loading
            loadingRequest.hide();
          }, function (e) {
            $scope.saveCubeRollBack();

            if (e.data && e.data.exception) {
              var message = e.data.exception;
              var msg = !!(message) ? message : 'Failed to take action.';
              MessageService.sendMsg($scope.cubeResultTmpl({
                'text': msg,
                'schema': $scope.state.cubeSchema
              }), 'error', {}, true, 'top_center');
            } else {
              MessageService.sendMsg($scope.cubeResultTmpl({
                'text': "Failed to take action.",
                'schema': $scope.state.cubeSchema
              }), 'error', {}, true, 'top_center');
            }
            //end loading
            loadingRequest.hide();

          });
        }
      }
      else {
        $scope.saveCubeRollBack();
      }
    });
  };


//    reverse the date
  $scope.saveCubeRollBack = function () {
  }

  $scope.updateMandatory = function (rowkey_column) {
    if (!rowkey_column.mandatory) {
      angular.forEach($scope.cubeMetaFrame.aggregation_groups, function (group, index) {
        var index = group.indexOf(rowkey_column.column);
        if (index > -1) {
          group.splice(index, 1);
        }
      });
    }
  }

  function reGenerateRowKey() {
    $log.log("reGen rowkey & agg group");
    //var fk_pk = {};
    var tmpRowKeyColumns = [];
    var tmpAggregationItems = [];//put all aggregation item
    //var hierarchyItemArray = [];//put all hierarchy items
    angular.forEach($scope.cubeMetaFrame.dimensions, function (dimension, index) {

      //derived column
      if (dimension.derived && dimension.derived.length) {
        var lookup = _.find($scope.metaModel.model.lookups, function (lookup) {
          return lookup.table == dimension.table
        });
        angular.forEach(lookup.join.foreign_key, function (fk, index) {
          for (var i = 0; i < tmpRowKeyColumns.length; i++) {
            if (tmpRowKeyColumns[i].column == fk)
              break;
          }
          // push to array if no duplicate value
          if (i == tmpRowKeyColumns.length) {
            tmpRowKeyColumns.push({
              "column": fk,
              "encoding": "dict"
            });

            tmpAggregationItems.push(fk);
          }
        })

      }
      //normal column
      else if (dimension.column  && !dimension.derived) {
        for (var i = 0; i < tmpRowKeyColumns.length; i++) {
          if (tmpRowKeyColumns[i].column == dimension.column)
            break;
        }
        if (i == tmpRowKeyColumns.length) {
          tmpRowKeyColumns.push({
            "column": dimension.column,
            "encoding": "dict"
          });
          tmpAggregationItems.push(dimension.column);
        }
      }

    });

    var rowkeyColumns = $scope.cubeMetaFrame.rowkey.rowkey_columns;
    var newRowKeyColumns = sortSharedData(rowkeyColumns, tmpRowKeyColumns);
    var increasedColumns = increasedColumn(rowkeyColumns, tmpRowKeyColumns);
    newRowKeyColumns = newRowKeyColumns.concat(increasedColumns);

    //! here get the latest rowkey_columns
    $scope.cubeMetaFrame.rowkey.rowkey_columns = newRowKeyColumns;

    if ($scope.cubeMode === "editExistCube") {
      //clear dims will not be used
      var aggregationGroups = $scope.cubeMetaFrame.aggregation_groups;
      rmDeprecatedDims(aggregationGroups,tmpAggregationItems);
    }

    if ($scope.cubeMode === "addNewCube") {

      //clear dims will not be used
      if($scope.cubeMetaFrame.aggregation_groups.length){
        var aggregationGroups = $scope.cubeMetaFrame.aggregation_groups;
        rmDeprecatedDims(aggregationGroups,tmpAggregationItems);
        return;
      }


      if (!tmpAggregationItems.length) {
        $scope.cubeMetaFrame.aggregation_groups = [];
        return;
      }

      var newUniqAggregationItem = [];
      angular.forEach(tmpAggregationItems, function (item, index) {
        if (newUniqAggregationItem.indexOf(item) == -1) {
          newUniqAggregationItem.push(item);
        }
      });

      $scope.cubeMetaFrame.aggregation_groups = [];
      var initJointGroups = sliceGroupItemToGroups(newUniqAggregationItem);
      var newGroup =  CubeDescModel.createAggGroup();
      newGroup.includes = newUniqAggregationItem;
      for(var i=1;i<initJointGroups.length;i++){
        if(initJointGroups[i].length>1){
          newGroup.select_rule.joint_dims[i-1] = initJointGroups[i];
        }
      }
      $scope.cubeMetaFrame.aggregation_groups.push(newGroup);

    }
  }

  function rmDeprecatedDims(aggregationGroups,tmpAggregationItems){
    angular.forEach(aggregationGroups, function (group, index) {
      if (group) {
        for (var j = 0; j < group.includes.length; j++) {
          var elemStillExist = false;
          for (var k = 0; k < tmpAggregationItems.length; k++) {
            if (group.includes[j] == tmpAggregationItems[k]) {
              elemStillExist = true;
              break;
            }
          }
          if (!elemStillExist) {
            var deprecatedItem = group.includes[j];
            //rm deprecated dimension from include
            group.includes.splice(j, 1);
            j--;

            //rm deprecated dimension in mandatory dimensions
            var mandatory = group.select_rule.mandatory_dims;
            if(mandatory && mandatory.length){
              var columnIndex = mandatory.indexOf(deprecatedItem);
              group.select_rule.mandatory_dims.splice(columnIndex,1);
            }

            var hierarchys =  group.select_rule.hierarchy_dims;
            if(hierarchys && hierarchys.length){
              for(var i=0;i<hierarchys.length;i++){
                var hierarchysIndex = hierarchys[i].indexOf(deprecatedItem);
                group.select_rule.hierarchy_dims[i].splice(hierarchysIndex,1);
              }

            }

            var joints =  group.select_rule.joint_dims;
            if(joints && joints.length){
              for(var i=0;i<joints.length;i++){
                var jointIndex = joints[i].indexOf(deprecatedItem);
                group.select_rule.joint_dims[i].splice(jointIndex,1);
              }

            }

          }
        }
      }
      else {
        aggregationGroups.splice(index, 1);
        index--;
      }
    });
  }

  function sortSharedData(oldArray, tmpArr) {
    var newArr = [];
    for (var j = 0; j < oldArray.length; j++) {
      var unit = oldArray[j];
      for (var k = 0; k < tmpArr.length; k++) {
        if (unit.column == tmpArr[k].column) {
          newArr.push(unit);
        }
      }
    }
    return newArr;
  }

  function increasedData(oldArray, tmpArr) {
    var increasedData = [];
    if (oldArray && !oldArray.length) {
      return increasedData.concat(tmpArr);
    }

    for (var j = 0; j < tmpArr.length; j++) {
      var unit = tmpArr[j];
      var exist = false;
      for (var k = 0; k < oldArray.length; k++) {
        if (unit == oldArray[k]) {
          exist = true;
          break;
        }
      }
      if (!exist) {
        increasedData.push(unit);
      }
    }
    return increasedData;
  }

  function increasedColumn(oldArray, tmpArr) {
    var increasedData = [];
    if (oldArray && !oldArray.length) {
      return increasedData.concat(tmpArr);
    }

    for (var j = 0; j < tmpArr.length; j++) {
      var unit = tmpArr[j];
      var exist = false;
      for (var k = 0; k < oldArray.length; k++) {
        if (unit.column == oldArray[k].column) {
          exist = true;
          break;
        }
      }
      if (!exist) {
        increasedData.push(unit);
      }
    }
    return increasedData;
  }

  function sliceGroupItemToGroups(groupItems) {
    if (!groupItems.length) {
      return [];
    }
    var groups = [];
    var j = -1;
    for (var i = 0; i < groupItems.length; i++) {
      if (i % 8 == 0) {
        j++;
        groups[j] = [];
      }
      groups[j].push(groupItems[i]);
    }
    return groups;
  }


  // ~ private methods
  function generateColumnFamily() {
    $scope.cubeMetaFrame.hbase_mapping.column_family = [];
    var colFamily = ColFamily();
    var normalMeasures = [], distinctCountMeasures = [];
    angular.forEach($scope.cubeMetaFrame.measures, function (measure, index) {
      if (measure.function.expression === 'COUNT_DISTINCT') {
        distinctCountMeasures.push(measure);
      } else {
        normalMeasures.push(measure);
      }
    });
    if (normalMeasures.length > 0) {
      var nmcf = colFamily();
      angular.forEach(normalMeasures, function (normalM, index) {
        nmcf.columns[0].measure_refs.push(normalM.name);
      });
      $scope.cubeMetaFrame.hbase_mapping.column_family.push(nmcf);
    }

    if (distinctCountMeasures.length > 0) {
      var dccf = colFamily();
      angular.forEach(distinctCountMeasures, function (dcm, index) {
        dccf.columns[0].measure_refs.push(dcm.name);
      });
      $scope.cubeMetaFrame.hbase_mapping.column_family.push(dccf);
    }
  }

  $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
    if(!$scope.projectModel.getSelectedProject()) {
      return;
    }
    var param = {
      ext: true,
      project: newValue
    };
    if (newValue) {
      TableModel.initTables();
      TableService.list(param, function (tables) {
        angular.forEach(tables, function (table) {
          table.name = table.database + "." + table.name;
          TableModel.addTable(table);
        });
      });
    }
  });

  $scope.$watch('cubeMetaFrame.model_name', function (newValue, oldValue) {
    if (!newValue) {
      return;
    }
    $scope.metaModel.model = modelsManager.getModel(newValue);
    if(!$scope.metaModel.model){
      return;
    }
  });


  $scope.$on('DimensionsEdited', function (event) {
    if ($scope.cubeMetaFrame) {
      reGenerateRowKey();
    }
  });
});
