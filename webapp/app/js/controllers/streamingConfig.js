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

KylinApp.controller('streamingConfigCtrl', function ($scope, $q, $routeParams, $location, $window, $modal, MessageService, CubeDescService, CubeService, JobService, UserService, ProjectService, SweetAlert, loadingRequest, $log, modelConfig, ProjectModel, ModelService, MetaModel, modelsManager, cubesManager, TableModel, $animate,StreamingService,StreamingModel) {

  $scope.addCluster = function () {
    $scope.kafkaMeta.clusters.push(StreamingModel.createKafkaCluster());
  };

  $scope.removeCluster = function(cluster){

    SweetAlert.swal({
      title: '',
      text: 'Are you sure to remove this cluster ?',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function(isConfirm) {
      if(isConfirm) {
        var index = $scope.kafkaMeta.clusters.indexOf(cluster);
        if (index > -1) {
          $scope.kafkaMeta.clusters.splice(index, 1);
        }
      }

    })
  }

  $scope.addBroker = function (cluster,broker) {
    //$scope.modelsManager.selectedModel = model;
    cluster.newBroker=(!!broker)?broker:StreamingModel.createBrokerConfig();
  };

  $scope.removeNewBroker = function (cluster){
    delete cluster.newBroker;
  }

  $scope.removeElement = function (cluster, element) {
    var index = cluster.brokers.indexOf(element);
    if (index > -1) {
      cluster.brokers.splice(index, 1);
    }
  };

  $scope.saveNewBroker = function(cluster){
    if (cluster.brokers.indexOf(cluster.newBroker) === -1) {
      cluster.brokers.push(cluster.newBroker);
    }
    delete cluster.newBroker;
  }

  $scope.clearNewBroker = function(cluster){
    delete cluster.newBroker;
  }


  $scope.streamingTsColUpdate = function(){
    if(!$scope.streamingCfg.parseTsColumn){
      $scope.streamingCfg.parseTsColumn = ' ';
    }
    $scope.kafkaMeta.parserProperties = "tsColName="+$scope.streamingCfg.parseTsColumn;
  }

});
