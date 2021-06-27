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

KylinApp.controller('CubeAdvanceSettingCtrl', function ($scope, $modal,cubeConfig,MetaModel,TableService,cubesManager,CubeDescModel,SweetAlert,VdmUtil,modelsManager) {
  $scope.cubesManager = cubesManager;

  var needLengthKeyList=cubeConfig.needSetLengthEncodingList;
  $scope.convertedRowkeys = [];
  $scope.dim_cap = $scope.cubeMetaFrame.aggregation_groups.length > 0 && $scope.cubeMetaFrame.aggregation_groups[0].select_rule.dim_cap ? $scope.cubeMetaFrame.aggregation_groups[0].select_rule.dim_cap : 0;

  TableService.list({ext: true, project:$scope.projectModel.selectedProject}, function(tables) {
    $scope.initRowKey(tables);
  }, function (error) {
    $scope.initRowKey([]);
  });

  $scope.initRowKey = function(tables) {
    angular.forEach($scope.cubeMetaFrame.rowkey.rowkey_columns,function(item){
      item.encoding=$scope.removeVersion(item.encoding);
      var _valueLength;
      var tableName=VdmUtil.getNameSpaceTopName(item.column);
      var databaseName=modelsManager.getDatabaseByColumnName(item.column);
      var baseKey=item.encoding.replace(/:\d+/,'');
      if(needLengthKeyList.indexOf(baseKey)>-1){
        var result=/:(\d+)/.exec(item.encoding);
        _valueLength=result?result[1]:0;
      }$scope.cubeMetaFrame
      var _encoding=baseKey;
      var rowkeyTable = _.find(tables, function(table) {
        var modelDesc = modelsManager.getModel($scope.cubeMetaFrame.model_name);
        var lookupTable = modelDesc ? _.find(modelDesc.lookups, function(lookup){ return lookup.alias === tableName; }) : undefined;
        return ((modelDesc && modelDesc.fact_table === (table.database + '.' + table.name) && table.name === tableName) || (lookupTable && lookupTable.table === (table.name + table.database)));
      });
      var cardinality = rowkeyTable ? rowkeyTable.cardinality[VdmUtil.removeNameSpace(item.column)] : undefined;
      var rowkeyObj = {
        column:item.column,
        encoding:_encoding+(item.encoding_version?"[v"+item.encoding_version+"]":"[v1]"),
        encodingName:_encoding,
        valueLength:_valueLength,
        isShardBy:item.isShardBy,
        encoding_version:item.encoding_version||1,
        table:tableName,
        database:databaseName,
        cardinality: cardinality || 'N/A'
      }
      if(item.index){
        rowkeyObj.index=item.index;
      }
      $scope.convertedRowkeys.push(rowkeyObj);
    })
  }


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
    var cardinality = item.cardinality;

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
    $scope.cubeMetaFrame.rowkey.rowkey_columns[index].cardinality = cardinality;
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
  $scope.sortableOptions = {
    stop:$scope.resortRowkey
  };
  $scope.changeDimCap  = function (dim_cap) {
    angular.forEach($scope.cubeMetaFrame.aggregation_groups, function (agg) {
      agg.select_rule.dim_cap = dim_cap
    })
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

  var ReuseEnum = {
    BUILD: 1,
    SELF: 2,
    DOMAIN: 3
  };

  $scope.isReuse=1;
  $scope.addNew=false;
  $scope.newDictionaries = {
    "column":null,
    "builder": null,
    "reuse": null,
    "model": null,
    "cube": null
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
      if(dictionaries.builder==null && dictionaries.model==null){
        $scope.isReuse = ReuseEnum.SELF;
      } else if (dictionaries.model!=null){
        $scope.isReuse = ReuseEnum.DOMAIN;
      }
      else{
        $scope.isReuse = ReuseEnum.BUILD;
      }
    }
    else{
      $scope.addNew=!$scope.addNew;
    }
    $scope.newDictionaries = (!!dictionaries)? jQuery.extend(true, {},dictionaries):CubeDescModel.createDictionaries();
  };

  $scope.saveNewDictionaries = function (){
    if($scope.updateDictionariesStatus.isEdit == true) {
      if ($scope.cubeMetaFrame.dictionaries[$scope.updateDictionariesStatus.editIndex].column != $scope.newDictionaries.column) {
        if(!$scope.checkColumn()){
          return false;
        }
      }
      $scope.cubeMetaFrame.dictionaries[$scope.updateDictionariesStatus.editIndex] = $scope.newDictionaries;
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
      $scope.isReuse = ReuseEnum.BUILD;
      return true;

  };

  $scope.nextDictionariesInit = function(){
    $scope.nextDic = {
      "coiumn":null,
      "builder":null,
      "reuse":null,
      "model":null,
      "cube":null
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
    $scope.isReuse = ReuseEnum.BUILD;
    $scope.initUpdateDictionariesStatus();
    $scope.nextDictionariesInit();
    $scope.addNew=!$scope.addNew;
  }

  $scope.change = function (type){
    $scope.newDictionaries.builder=null;
    $scope.newDictionaries.reuse=null;
    $scope.newDictionaries.domain=null;
    $scope.newDictionaries.model=null;
    $scope.newDictionaries.cube=null;
    if(type == 'domain'){
      $scope.isReuse = ReuseEnum.DOMAIN;
    }else if (type == 'builder'){
      $scope.isReuse = ReuseEnum.BUILD;
    }else if (type == 'reuse'){
      $scope.isReuse = ReuseEnum.SELF;
    }
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

    for(var j=0;j<$scope.cubeMetaFrame.hbase_mapping.column_family.length; j++) {
      for (var i=0;i<$scope.cubeMetaFrame.hbase_mapping.column_family[j].columns[0].measure_refs.length; i++){
        var allIndex = allMeasureNames.indexOf($scope.cubeMetaFrame.hbase_mapping.column_family[j].columns[0].measure_refs[i]);
        if (allIndex == -1) {
          tmpColumnFamily[j].columns[0].measure_refs.splice(i, 1);
          i--
        }
      }
      if (tmpColumnFamily[j].columns[0].measure_refs.length == 0) {
        tmpColumnFamily.splice(j, 1);
        j--
      }
    }

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

  $scope.mandatoryDimensionSet = {
    select: []
  };

  $scope.uploadMandatoryDimensionSetList = function() {
    var file = document.getElementById('cuboids').files[0];
    if (file) {
      var reader = new FileReader();
      reader.onload = function(event) {
        var dimensionSetList = JSON.parse(event.target.result);
        $scope.cubeMetaFrame.mandatory_dimension_set_list = dimensionSetList;
        $scope.$apply();
        // TODO add verify dimension set
      };
      reader.readAsText(file);
    } else {
      swal('Oops...', 'Please choose your file first.', 'warning');
    }
  };

  $scope.removeDimensionSet = function(index) {
    $scope.cubeMetaFrame.mandatory_dimension_set_list.splice(index, 1);
  };

  $scope.addDimensionSet = function() {
    if ($scope.mandatoryDimensionSet.select.length) {
      // validate the dimension set existed
      var existed = false;
      var selectedDimension = _.clone($scope.mandatoryDimensionSet.select).sort(function (dimensionA, dimensionB) {
        if (dimensionA < dimensionB) return 1;
        if (dimensionB < dimensionA) return -1;
        return 0;
      });
      angular.forEach($scope.cubeMetaFrame.mandatory_dimension_set_list, function(dimensionSet, index) {
        var dimensionSetSorted = _.clone(dimensionSet).sort(function (dimensionA, dimensionB) {
          if (dimensionA < dimensionB) return 1;
          if (dimensionB < dimensionA) return -1;
          return 0;
        });
        if (angular.equals(dimensionSet, selectedDimension)) {
          existed = true;
        };
      });
      if (!existed) {
        $scope.cubeMetaFrame.mandatory_dimension_set_list.push($scope.mandatoryDimensionSet.select);
        $scope.mandatoryDimensionSet.select = [];
      } else {
        swal('Oops...', 'Dimension set already existed', 'warning');
      }
    } else {
      swal('Oops...', 'Dimension set should not be empty', 'warning');
    }
  };

  if ($scope.state.mode == 'edit') {
    $scope.$on('$destroy', function () {
      $scope.$emit('AdvancedSettingEdited');
    });
  }

  $scope.newSnapshot = {
    select: {}
  };

  $scope.removeSnapshotTable = function(index) {
    $scope.cubeMetaFrame.snapshot_table_desc_list.splice(index, 1);
  };

  $scope.addSnapshot = function(newSnapshot) {
    if (!$scope.cubeMetaFrame.snapshot_table_desc_list) {
       $scope.cubeMetaFrame.snapshot_table_desc_list = [];
    }
    if (!newSnapshot.table_name || !newSnapshot.storage_type) {
      swal('Oops...', 'Snapshot table name or storage should not be empty', 'warning');
      return;
    } else if ($scope.cubeMetaFrame.snapshot_table_desc_list.length && newSnapshot.editIndex == null){
      var existSnapshot = _.find($scope.cubeMetaFrame.snapshot_table_desc_list, function(snapshot){ return snapshot.table_name === newSnapshot.table_name;});
      if (!!existSnapshot) {
        swal('Oops...', 'Snapshot table already existed', 'warning');
        return;
      }
    }
    if (newSnapshot.editIndex != null) {
      $scope.cubeMetaFrame.snapshot_table_desc_list[newSnapshot.editIndex] = angular.copy(newSnapshot);
    } else {
      $scope.cubeMetaFrame.snapshot_table_desc_list.push(angular.copy(newSnapshot));
    }
    $scope.newSnapshot.select = {};
    $scope.addNewSanpshot = !$scope.addNewSanpshot;
  };

  $scope.changeSnapshotStorage = function(snapshot) {
    if (snapshot.storage_type == 'hbase') {
      snapshot.global = true;
    }
  };

  $scope.changeSnapshotTable = function(changeSnapshot, beforeTableName, snapshotTableDescList) {
    var existSnapshot = _.find(snapshotTableDescList, function(snapshot) {
      return snapshot.table_name === changeSnapshot.table_name;
    });
    if (!!existSnapshot) {
      changeSnapshot.table_name = beforeTableName;
      swal('Oops...', 'Snapshot table already existed', 'warning');
    }
  };

  $scope.addNewSnapshot = function(sanpshot, index) {
    if (sanpshot && index >=0) {
      $scope.newSnapshot.select = sanpshot;
      $scope.addNewSanpshot = true;
      $scope.newSnapshot.select.editIndex = index;
    } else {
      $scope.addNewSanpshot = !$scope.addNewSanpshot;
    }
  };

  $scope.cancelEditSnapshot = function() {
    $scope.newSnapshot.select = {};
    $scope.addNewSanpshot = !$scope.addNewSanpshot;
  };

  $scope.getCubeLookups = function() {
    var modelDesc = modelsManager.getModel($scope.cubeMetaFrame.model_name);
    var modelLookups = modelDesc ? modelDesc.lookups : [];
    var cubeLookups = [];
    angular.forEach(modelLookups, function(modelLookup, index) {
      var dimensionLookup = _.find($scope.cubeMetaFrame.dimensions, function(dimension){ return dimension.table === modelLookup.alias;});
      if (!!dimensionLookup) {
        if (cubeLookups.indexOf(modelLookup.table) === -1) {
          cubeLookups.push(modelLookup.table);
        }
      }
    });
    return cubeLookups;
  };

  $scope.isAvailableEngine = function(engine_type) {
    return !($scope.cubeMetaFrame.storage_type === 3 && engine_type.value === 4);
  }

  $scope.cubeLookups = $scope.getCubeLookups();
});
