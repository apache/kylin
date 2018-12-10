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

KylinApp.controller('CubeOverWriteCtrl', function ($scope, $modal,cubeConfig,MetaModel,cubesManager,CubeDescModel,TableService,tableConfig) {
  $scope.cubesManager = cubesManager;

  // check the cube is streaming
  $scope.isStreamingCube = $scope.cubeMetaFrame.storage_type === 3;

  // Set default value for streaming properties
  if ($scope.state.mode === 'edit' && $scope.isStreamingCube) {
    if (!$scope.cubeMetaFrame.override_kylin_properties['kylin.stream.cube.window']) {
      $scope.cubeMetaFrame.override_kylin_properties['kylin.stream.cube.window'] = 3600;
    }
    if (!$scope.cubeMetaFrame.override_kylin_properties['kylin.stream.cube.duration']) {
      $scope.cubeMetaFrame.override_kylin_properties['kylin.stream.cube.duration'] = 3600;
    }
    if (!$scope.cubeMetaFrame.override_kylin_properties['kylin.stream.index.checkpoint.intervals']) {
      $scope.cubeMetaFrame.override_kylin_properties['kylin.stream.index.checkpoint.intervals'] = 300;
    }
    if (!$scope.cubeMetaFrame.override_kylin_properties['kylin.cube.algorithm']) {
      $scope.cubeMetaFrame.override_kylin_properties['kylin.cube.algorithm'] = 'INMEM';
    }
    if (!$scope.cubeMetaFrame.override_kylin_properties['kylin.stream.segment.retention.policy']) {
      $scope.cubeMetaFrame.override_kylin_properties['kylin.stream.segment.retention.policy'] = 'fullBuild';
    }
  }

  //rowkey
  $scope.convertedProperties = [];

  for (var key in $scope.cubeMetaFrame.override_kylin_properties) {
    var streamingProperties = [];
    if ($scope.isStreamingCube) {
      streamingProperties = ['kylin.stream.cube.window', 'kylin.stream.cube.duration', 'kylin.stream.index.checkpoint.intervals', 'kylin.cube.algorithm', 'kylin.stream.segment.retention.policy', 'kylin.stream.segment.retention.policy.purge.retentionTimeInSec'];
    }
    if (streamingProperties.indexOf(key) === -1) {
      $scope.convertedProperties.push({
        name:key,
        value:$scope.cubeMetaFrame.override_kylin_properties[key]
      });
    }
  }


  $scope.addNewProperty = function () {
    if($scope.cubeMetaFrame.override_kylin_properties.hasOwnProperty('')){
      return;
    }
    $scope.cubeMetaFrame.override_kylin_properties['']='';
    $scope.convertedProperties.push({
      name:'',
      value:''
    });

  };

  $scope.refreshPropertiesObj = function(){
    if ($scope.isStreamingCube) {
      // keep the streaming setting
      for(var prop in $scope.cubeMetaFrame.override_kylin_properties) {
        if(streamingProperties.indexOf(prop) === -1) {
          delete $scope.cubeMetaFrame.override_kylin_properties[prop];
        }
      }
    } else {
      $scope.cubeMetaFrame.override_kylin_properties = {};
    }
    angular.forEach($scope.convertedProperties,function(item,index){
      $scope.cubeMetaFrame.override_kylin_properties[item.name] = item.value;
    })
  }


  $scope.refreshProperty = function(list,index,item){
    $scope.convertedProperties[index] = item;
    $scope.refreshPropertiesObj();
  }


  $scope.removeProperty= function(arr,index,item){
    if (index > -1) {
      arr.splice(index, 1);
    }
    delete $scope.cubeMetaFrame.override_kylin_properties[item.name];
  }

  $scope.changeStreamingRetentionPolicy = function(policy) {
    if (policy === 'fullBuild' && $scope.cubeMetaFrame.override_kylin_properties['kylin.stream.segment.retention.policy.purge.retentionTimeInSec']) {
      delete $scope.cubeMetaFrame.override_kylin_properties['kylin.stream.segment.retention.policy.purge.retentionTimeInSec'];
    }
    if (policy === 'purge') {
      $scope.cubeMetaFrame.override_kylin_properties['kylin.stream.segment.retention.policy.purge.retentionTimeInSec'] = 86400;
    }
  };

});
