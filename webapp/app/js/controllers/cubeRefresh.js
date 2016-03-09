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

KylinApp.controller('CubeRefreshCtrl', function ($scope, $modal,cubeConfig,MetaModel,UserService) {
  $scope.userService = UserService;

  //edit model
  if($scope.state.mode==="edit") {
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

  if ($scope.state.mode == 'edit') {
    $scope.$on('$destroy', function () {
      $scope.cubeMetaFrame.auto_merge_time_ranges.sort(function(a, b){return a-b});;
    });
  }
});
