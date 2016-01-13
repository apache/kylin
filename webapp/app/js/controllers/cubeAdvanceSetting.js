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

KylinApp.controller('CubeAdvanceSettingCtrl', function ($scope, $modal,cubeConfig,MetaModel,cubesManager,CubeDescModel) {
    $scope.cubesManager = cubesManager;

    //edit model
    if($scope.state.mode==="edit") {
        $scope.metaModel = MetaModel;
      if(!$scope.cubeMetaFrame.auto_merge_time_ranges){
        $scope.cubeMetaFrame.auto_merge_time_ranges = [];
      }
      $scope._auto_merge_time_ranges = [];
      angular.forEach($scope.cubeMetaFrame.auto_merge_time_ranges,function(item){
        var _day = Math.floor(item/86400000);
        var _hour = (item%86400000)/3600000;

        var rangeObj = {
          type:'days',
          range:0,
          mills:0
        }
        if(_day==0){
          rangeObj.type = 'hours';
          rangeObj.range = _hour;
          rangeObj.mills = rangeObj.range*3600000;
         }else{
          rangeObj.type = 'days';
          rangeObj.range = _day;
          rangeObj.mills = rangeObj.range*86400000;
        }
        $scope._auto_merge_time_ranges.push(rangeObj);
      })
    }

  //rowkey
  $scope.convertedRowkeys = [];
  angular.forEach($scope.cubeMetaFrame.rowkey.rowkey_columns,function(item){
    var _isDictionary = item.encoding === "dict"?"true":"false";
    var _isFixedLength = item.encoding.substring(0,12) === "fixed_length"?"true":"false";//fixed_length:12
    var _fixedLength ;
    if(_isFixedLength){
      _fixedLength = item.encoding.substring(12,item.encoding.length);
    }
    var rowkeyObj = {
      column:item.column,
      isDictionary:_isDictionary,
      isFixedLength:_isFixedLength,
      fixedLength:_fixedLength
    }

    $scope.convertedRowkeys.push(rowkeyObj);

  })

  $scope.addNewMergeTimeRange = function(){
    $scope._auto_merge_time_ranges.push({
      type:'days',
      range:0,
      mills:0
    })
    $scope.updateAutoMergeRange();
  }

  $scope.removeTimeRange = function(arr,index,item){
    if (index > -1) {
      arr.splice(index, 1);
    }
    $scope.cubeMetaFrame.auto_merge_time_ranges.splice(index,1);
  }


  $scope.refreshAutoMergeTimeRanges = function(list,index,item){
    if(item.type=='hours'){
      item.mills = item.range*3600000;
    }else{
      item.mills = item.range*86400000;
    }
    $scope.cubeMetaFrame.auto_merge_time_ranges[index] = item.mills;
  }

  $scope.updateAutoMergeRange = function(){
    $scope.cubeMetaFrame.auto_merge_time_ranges = [];
    angular.forEach($scope._auto_merge_time_ranges,function(item){
      $scope.cubeMetaFrame.auto_merge_time_ranges.push(item.mills);
    })
  }

  $scope.refreshRowKey = function(list,index,item){
    var encoding = "dict";
    var column = item.column;
    if(item.isDictionary!=="true"){
      if(item.fixedLength){
        encoding = "fixed_length:"+item.fixedLength;
      }
    }
    $scope.cubeMetaFrame.rowkey.rowkey_columns[index].column = column;
    $scope.cubeMetaFrame.rowkey.rowkey_columns[index].encoding = encoding;

  }

  $scope.removeRowkey = function(arr,index,item){
    if (index > -1) {
      arr.splice(index, 1);
    }
    $scope.cubeMetaFrame.rowkey.rowkey_columns.splice(index,1);
  }


  $scope.addNewRowkeyColumn = function () {
    var rowkeyObj = {
      column:"",
      isDictionary:"true",
      isFixedLength:"false",
      fixedLength:0
    }

    $scope.convertedRowkeys.push(rowkeyObj);
    $scope.cubeMetaFrame.rowkey.rowkey_columns.push({
      column:'',
      encoding:'dict'
    });

  };
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
    console.log($scope.cubeMetaFrame.aggregation_groups);
  };

  $scope.refreshAggregationHierarchy = function (list, index, aggregation_group,hieIndex,hierarchy) {
    if(hierarchy){
      aggregation_group.select_rule.hierarchy_dims[hieIndex] = hierarchy;
    }
    if (aggregation_group) {
      list[index] = aggregation_group;
    }
    console.log($scope.cubeMetaFrame.aggregation_groups);
  };

  $scope.refreshAggregationJoint = function (list, index, aggregation_group,joinIndex,jointDim){
    if(jointDim){
      aggregation_group.select_rule.joint_dims[joinIndex] = jointDim;
    }
    if (aggregation_group) {
      list[index] = aggregation_group;
    }
    console.log($scope.cubeMetaFrame.aggregation_groups);
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

});
