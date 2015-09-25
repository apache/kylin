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

KylinApp.service('StreamingList', function (CubeService, $q, AccessService, StreamingService) {
  var _this = this;
  this.streamingConfigs = [];
  this.kafkaConfigs = [];


  this.list = function () {
    var defer = $q.defer();

    var streamingPromises = [];
    var kafkaPromises = [];


    kafkaPromises.push(StreamingService.getKfkConfig({}, function (kfkConfigs) {
      _this.kafkaConfigs = kfkConfigs;
    },function(){
      defer.reject("Failed to load models");
    }).$promise);

    streamingPromises.push(StreamingService.getConfig({}, function (streamings) {
      _this.streamingConfigs = streamings;
    },function(){
      defer.reject("Failed to load models");
    }).$promise);

    $q.all(streamingPromises,kafkaPromises).then(function(result,rs){
      defer.resolve("success");
    },function(){
      defer.resolve("failed");
    })
    return defer.promise;

  };

  this.checkCubeExist = function(cubeName){
    var result = {streaming:null,exist:false};
    for(var i=0;i<_this.streamingConfigs.length;i++){
      if(_this.streamingConfigs[i].cubeName == cubeName){
        result ={
          streaming:_this.streamingConfigs[i],
          exist:true
        }
        break;
      }
    }
    return result;
  }

  this.getKafkaConfig = function(kfkName){
      for(var i=0;i<_this.kafkaConfigs.length;i++) {
        if(_this.kafkaConfigs[i].name == kfkName){
          return _this.kafkaConfigs[i];
        }
      }
    }

  this.removeAll = function () {
    _this.streamingConfigs = [];
    _this.kafkaConfigs = [];
  };

});
