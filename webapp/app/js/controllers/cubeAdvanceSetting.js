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

KylinApp.controller('CubeAdvanceSettingCtrl', function ($scope, $modal,cubeConfig,MetaModel,cubesManager,CubeDescModel,SweetAlert,VdmUtil,modelsManager) {
  $scope.cubesManager = cubesManager;

  var needLengthKeyList=cubeConfig.needSetLengthEncodingList;
  $scope.convertedRowkeys = [];
  angular.forEach($scope.cubeMetaFrame.rowkey.rowkey_columns,function(item){
    item.encoding=$scope.removeVersion(item.encoding);
    var _valueLength;
    var tableName=VdmUtil.getNameSpaceTopName(item.column);
    var databaseName=modelsManager.getDatabaseByColumnName(item.column);
    var baseKey=item.encoding.replace(/:\d+/,'');
    if(needLengthKeyList.indexOf(baseKey)>-1){
      var result=/:(\d+)/.exec(item.encoding);
      _valueLength=result?result[1]:0;
    }
    var _encoding=baseKey;
    var rowkeyObj = {
      column:item.column,
      encoding:_encoding+(item.encoding_version?"[v"+item.encoding_version+"]":"[v1]"),
      encodingName:_encoding,
      valueLength:_valueLength,
      isShardBy:item.isShardBy,
      encoding_version:item.encoding_version||1,
      table:tableName,
      database:databaseName
    }
    if(item.index){
      rowkeyObj.index=item.index;
    }
    $scope.convertedRowkeys.push(rowkeyObj);

  })


  $scope.rule={
    shardColumnAvailable:true
  }
  var checkedlen=$scope.cubeMetaFrame.rowkey.rowkey_columns&&$scope.cubeMetaFrame.rowkey.rowkey_columns.length||0;
  for(var i=0;i<checkedlen;i++){
    $scope.cubeMetaFrame.rowkey.rowkey_columns[i].encoding_version=$scope.cubeMetaFrame.rowkey.rowkey_columns[i].encoding_version||1;
  }
  $scope.refreshRowKey = function(list,index,item,checkShard){
    var encoding;
    var column = item.column;
    var isShardBy = item.isShardBy;
    var version=$scope.getTypeVersion(item.encoding);
    var encodingType=$scope.removeVersion(item.encoding);

    if(needLengthKeyList.indexOf(encodingType)!=-1){
      encoding = encodingType+":"+item.valueLength;
    }else{
      encoding = encodingType;
      item.valueLength=0;
    }
    $scope.cubeMetaFrame.rowkey.rowkey_columns[index].column = column;
    $scope.cubeMetaFrame.rowkey.rowkey_columns[index].encoding = encoding;
    $scope.cubeMetaFrame.rowkey.rowkey_columns[index].encoding_version =version;
    $scope.cubeMetaFrame.rowkey.rowkey_columns[index].isShardBy = isShardBy;
    if(checkShard == true){
      $scope.checkShardByColumn();
    }
  }

  $scope.checkShardByColumn = function(){
    var shardRowkeyList = [];
    angular.forEach($scope.cubeMetaFrame.rowkey.rowkey_columns,function(rowkey){
      if(rowkey.isShardBy == true){
        shardRowkeyList.push(rowkey.column);
      }
    })
    if(shardRowkeyList.length >1){
      $scope.rule.shardColumnAvailable = false;
    }else{
      $scope.rule.shardColumnAvailable = true;
    }
  }


  $scope.resortRowkey = function(){
    for(var i=0;i<$scope.convertedRowkeys.length;i++){
      $scope.refreshRowKey($scope.convertedRowkeys,i,$scope.convertedRowkeys[i]);
    }
  }

  $scope.addNewHierarchy = function(grp){
    grp.select_rule.hierarchy_dims.push([]);
  }

  $scope.addNewJoint = function(grp){
    grp.select_rule.joint_dims.push([]);
  }

  //to do, agg update
  $scope.addNewAggregationGroup = function () {
    $scope.cubeMetaFrame.aggregation_groups.push(CubeDescModel.createAggGroup());
  };

  $scope.refreshAggregationGroup = function (list, index, aggregation_groups) {
    if (aggregation_groups) {
      list[index] = aggregation_groups;
    }
  };

  $scope.refreshAggregationHierarchy = function (list, index, aggregation_group,hieIndex,hierarchy) {
    if(hierarchy){
      aggregation_group.select_rule.hierarchy_dims[hieIndex] = hierarchy;
    }
    if (aggregation_group) {
      list[index] = aggregation_group;
    }
  };

  $scope.refreshAggregationJoint = function (list, index, aggregation_group,joinIndex,jointDim){
    if(jointDim){
      aggregation_group.select_rule.joint_dims[joinIndex] = jointDim;
    }
    if (aggregation_group) {
      list[index] = aggregation_group;
    }
  };

  $scope.refreshIncludes = function (list, index, aggregation_groups) {
    if (aggregation_groups) {
      list[index] = aggregation_groups;
    }
  };

  $scope.removeElement = function (arr, element) {
    var index = arr.indexOf(element);
    if (index > -1) {
      arr.splice(index, 1);
    }
  };

  $scope.removeHierarchy = function(arr,element){
    var index = arr.select_rule.hierarchy_dims.indexOf(element);
    if(index>-1){
      arr.select_rule.hierarchy_dims.splice(index,1);
    }

  }

  $scope.removeJointDims = function(arr,element){
    var index = arr.select_rule.joint_dims.indexOf(element);
    if(index>-1){
      arr.select_rule.joint_dims.splice(index,1);
    }

  }

  $scope.isReuse=false;
  $scope.addNew=false;
  $scope.newDictionaries = {
    "column":null,
    "builder": null,
    "reuse": null
  }

  $scope.initUpdateDictionariesStatus = function(){
    $scope.updateDictionariesStatus = {
      isEdit:false,
      editIndex:-1
    }
  };
  $scope.initUpdateDictionariesStatus();


  $scope.addNewDictionaries = function (dictionaries, index) {
    if(dictionaries&&index>=0){
      $scope.updateDictionariesStatus.isEdit = true;
      $scope.addNew=true;
      $scope.updateDictionariesStatus.editIndex = index;
      if(dictionaries.builder==null){
        $scope.isReuse=true;
      }
      else{
        $scope.isReuse=false;
      }
    }
    else{
      $scope.addNew=!$scope.addNew;
    }
    $scope.newDictionaries = (!!dictionaries)? jQuery.extend(true, {},dictionaries):CubeDescModel.createDictionaries();
  };

  $scope.saveNewDictionaries = function (){
    if(!$scope.cubeMetaFrame.dictionaries){
      $scope.cubeMetaFrame.dictionaries=[];
    }

    if($scope.updateDictionariesStatus.isEdit == true) {
      if ($scope.cubeMetaFrame.dictionaries[$scope.updateDictionariesStatus.editIndex].column != $scope.newDictionaries.column) {
        if(!$scope.checkColumn()){
          return false;
        }
      }
      else {
        $scope.cubeMetaFrame.dictionaries[$scope.updateDictionariesStatus.editIndex] = $scope.newDictionaries;
      }
    }
    else
      {
        if(!$scope.checkColumn()){
        return false;
        }
        $scope.cubeMetaFrame.dictionaries.push($scope.newDictionaries);
      }
      $scope.newDictionaries = null;
      $scope.initUpdateDictionariesStatus();
      $scope.nextDictionariesInit();
      $scope.addNew = !$scope.addNew;
      $scope.isReuse = false;
      return true;

  };

  $scope.nextDictionariesInit = function(){
    $scope.nextDic = {
      "coiumn":null,
      "builder":null,
      "reuse":null
    }
  }

  $scope.checkColumn = function (){
    var isColumnExit=false;
        angular.forEach($scope.cubeMetaFrame.dictionaries,function(dictionaries){
          if(!isColumnExit){
            if(dictionaries.column==$scope.newDictionaries.column)
              isColumnExit=true;
          }
        })
    if(isColumnExit){
      SweetAlert.swal('Oops...', "The column named [" + $scope.newDictionaries.column + "] already exists", 'warning');
      return false;
    }
    return true;
  }

  $scope.clearNewDictionaries = function (){
    $scope.newDictionaries = null;
    $scope.isReuse=false;
    $scope.initUpdateDictionariesStatus();
    $scope.nextDictionariesInit();
    $scope.addNew=!$scope.addNew;
  }

  $scope.change = function (){
    $scope.newDictionaries.builder=null;
    $scope.newDictionaries.reuse=null;
    $scope.isReuse=!$scope.isReuse;
  }

  $scope.removeElement =  function(arr,element){
    var index = arr.indexOf(element);
    if (index > -1) {
      arr.splice(index, 1);
    }
  };

  $scope.newColFamily = function (index) {
    return {
        "name": "F" + index,
        "columns": [
          {
            "qualifier": "M",
            "measure_refs": []
          }
        ]
      };
  };

  $scope.initColumnFamily = function () {
    $scope.cubeMetaFrame.hbase_mapping.column_family = [];
    var normalMeasures = [], distinctCountMeasures = [];
    angular.forEach($scope.cubeMetaFrame.measures, function (measure, index) {
      if (measure.function.expression === 'COUNT_DISTINCT') {
        distinctCountMeasures.push(measure);
      } else {
        normalMeasures.push(measure);
      }
    });
    if (normalMeasures.length > 0) {
      var nmcf = $scope.newColFamily(1);
      angular.forEach(normalMeasures, function (normalM, index) {
        nmcf.columns[0].measure_refs.push(normalM.name);
      });
      $scope.cubeMetaFrame.hbase_mapping.column_family.push(nmcf);
    }

    if (distinctCountMeasures.length > 0) {
      var dccf = $scope.newColFamily(2);
      angular.forEach(distinctCountMeasures, function (dcm, index) {
        dccf.columns[0].measure_refs.push(dcm.name);
      });
      $scope.cubeMetaFrame.hbase_mapping.column_family.push(dccf);
    }
  };

  $scope.getAllMeasureNames = function () {
    var measures = [];
    angular.forEach($scope.cubeMetaFrame.measures, function (measure, index) {
      measures.push(measure.name);
    });
    return measures;
  };

  $scope.getAssignedMeasureNames = function () {
    var assignedMeasures = [];
    angular.forEach($scope.cubeMetaFrame.hbase_mapping.column_family, function (colFamily, index) {
      angular.forEach(colFamily.columns[0].measure_refs, function (measure, index) {
        assignedMeasures.push(measure);
      });
    });
    return assignedMeasures;
  };

  $scope.rmDeprecatedMeasureNames = function () {
    var allMeasureNames = $scope.getAllMeasureNames();
    var tmpColumnFamily = $scope.cubeMetaFrame.hbase_mapping.column_family;

    angular.forEach($scope.cubeMetaFrame.hbase_mapping.column_family, function (colFamily,index1) {
      angular.forEach(colFamily.columns[0].measure_refs, function (measureName, index2) {
        var allIndex = allMeasureNames.indexOf(measureName);
        if (allIndex == -1) {
          tmpColumnFamily[index1].columns[0].measure_refs.splice(index2, 1);
        }

        if (tmpColumnFamily[index1].columns[0].measure_refs == 0) {
          tmpColumnFamily.splice(index1, 1);
        }
      });
    });

    $scope.cubeMetaFrame.hbase_mapping.column_family = tmpColumnFamily;
  };

  if ($scope.getAssignedMeasureNames().length == 0) {
    $scope.initColumnFamily();
  } else {
    $scope.rmDeprecatedMeasureNames();
    if ($scope.getAllMeasureNames().length > $scope.getAssignedMeasureNames().length) {
      $scope.initColumnFamily();
    }
  }

  $scope.addColumnFamily = function () {
    var isCFEmpty = _.some($scope.cubeMetaFrame.hbase_mapping.column_family, function(colFamily) {
      return colFamily.columns[0].measure_refs.length == 0;
    });

    if (isCFEmpty === true) {
      return;
    }

    var colFamily = $scope.newColFamily($scope.cubeMetaFrame.hbase_mapping.column_family.length + 1);
    $scope.cubeMetaFrame.hbase_mapping.column_family.push(colFamily);
  };

  $scope.refreshColumnFamily = function (column_familys, index, colFamily) {
    if (column_familys) {
      column_familys[index] = colFamily;
    }
  };

});
