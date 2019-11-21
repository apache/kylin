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

KylinApp.controller('AdminStreamingCtrl', function ($scope, $timeout, $modal, AdminStreamingService, MessageService, loadingRequest, UserService, ProjectModel, SweetAlert, $filter) {
  //TODO move config to model dir
  var config = liquidFillGaugeDefaultSettings();
  config.waveCount = 3;
  config.waveAnimate = false;
  config.waveRise = false;
  config.waveHeightScaling = false;
  config.waveOffset = 0.25;
  config.textSize = 0.9;
  config.textVertPosition = 0.7;
  config.waveHeight = 0;
  config.displayPercent = false;
  config.circleColor = '#00a65a';
  config.waveColor = '#00a65a';
  config.textColor = '#000000';
  config.waveTextColor = '#ffffff';

  config.maxValue = 20000 // TODO change it to configurable

  var inactiveConfig = angular.copy(config);
  inactiveConfig.circleColor = '#dd4b39';
  inactiveConfig.waveColor = '#dd4b39';

  var unreachableConfig = angular.copy(config);
  unreachableConfig.circleColor = '#d2d6de';
  unreachableConfig.waveColor = '#d2d6de';

  var warningConfig = angular.copy(config);
  warningConfig.circleColor = '#f39c12';
  warningConfig.waveColor = '#f39c12';

  var availableConfig = angular.copy(config);
  availableConfig.circleColor = '#3c8dbc';
  availableConfig.waveColor = '#3c8dbc';

  $scope.listReplicaSet = function(callback) {
    AdminStreamingService.getClusterState({}, function(data) {
      $scope.replicaSetStates = data.rs_states;
      $scope.availableReceiver = data.available_receivers;
      $timeout(function() {
        angular.forEach($scope.replicaSetStates, function(replicaSetState, rsIndex) {
          var rsId = 'rs-' + replicaSetState.rs_id;
          angular.forEach(replicaSetState.receiver_states, function(receiverState, reIndex) {
            var reId = rsId + '-re-' + $filter('formatId')(receiverState.receiver.host) + '_' + receiverState.receiver.port;
            if (!$scope[reId]) {
              drawLiquidChart(reId, receiverState.rate, receiverState.state);
            } else {
              delete $scope[reId];
              drawLiquidChart(reId, receiverState.rate, receiverState.state);
            }
          });
        });
        angular.forEach($scope.availableReceiver, function(receiverState, reIndex) {
          var reId = 're-' + $filter('formatId')(receiverState.receiver.host) + '_' + receiverState.receiver.port;
          if (!$scope[reId]) {
            drawLiquidChart(reId, receiverState.rate, receiverState.state, 'isAvailable');
          } else {
            delete $scope[reId];
            drawLiquidChart(reId, receiverState.rate, receiverState.state, 'isAvailable');
          }
        });
        callback && callback();
      }, 100);
    }, function(e) {
      if (e.data && e.data.exception) {
        var message = e.data.exception;
        var msg = !!(message) ? message : 'Failed get replica set';
        SweetAlert.swal('Oops...', msg, 'error');
      } else {
        SweetAlert.swal('Oops...', 'Failed get replica set', 'error');
      }
      callback && callback();
    });
  };

  function drawLiquidChart(reId, rate, state, isAvailable) {
    var value = Number(rate).toFixed(0);
    if ('DOWN' === state) {
      $scope[reId] = loadLiquidFillGauge(reId, value, angular.copy(inactiveConfig));
    } else if ('UNREACHABLE' === state) {
      $scope[reId] = loadLiquidFillGauge(reId, value, angular.copy(unreachableConfig));
    } else if ('WARN' === state) {
      $scope[reId] = loadLiquidFillGauge(reId, value, angular.copy(warningConfig));
    } else {
      if (isAvailable === 'isAvailable') {
        $scope[reId] = loadLiquidFillGauge(reId, value, angular.copy(availableConfig));
      } else {
        $scope[reId] = loadLiquidFillGauge(reId, value, angular.copy(config));
      }
    }
  };

  $scope.editReplicaSet = function(replicaSet){
    $modal.open({
      templateUrl: 'editReplicaSet.html',
      controller: function ($scope, scope, $modalInstance, replicaSet, AdminStreamingService, availableReceiver) {
        $scope.replicaSet = replicaSet;
        $scope.availableNodes = availableReceiver;

        $scope.node = {
          selected : ''
        };

        $scope.cancel = function () {
          $modalInstance.dismiss('cancel');
        };

        $scope.removeReceiver = function(receiver) {
          var nodeId = receiver.receiver.host + '_' + receiver.receiver.port;
          if (nodeId !== '_') {
            loadingRequest.show();
            AdminStreamingService.removeNodeToReplicaSet({replicaSetId: $scope.replicaSet.rs_id, nodeId: nodeId}, {}, function(data) {
              AdminStreamingService.getClusterState({},function(data) {
                var newReplicaSet = _.find(data.rs_states, function(item){
                  return item.rs_id == $scope.replicaSet.rs_id;
                });
                $scope.replicaSet = newReplicaSet;
                scope.listReplicaSet(function() {
                  $scope.availableNodes = scope.availableReceiver;
                  loadingRequest.hide();
                });
              }, function(e) {
                scope.listReplicaSet(function() {
                  $scope.availableNodes = scope.availableReceiver;
                  loadingRequest.hide();
                });
                errorMessage(e, 'Failed get replica set');
              });
            }, function(e) {
              scope.listReplicaSet(function() {
                loadingRequest.hide();
              });
              errorMessage(e, 'Failed remove receiver');
            });
          }
        };

        $scope.addNodeToReplicaSet = function() {
          if ($scope.node.selected) {
            loadingRequest.show();
            AdminStreamingService.addNodeToReplicaSet({replicaSetId: $scope.replicaSet.rs_id, nodeId: $scope.node.selected}, {}, function(data) {
              AdminStreamingService.getClusterState({},function(data) {
                var newReplicaSet = _.find(data.rs_states, function(item){
                  return item.rs_id == $scope.replicaSet.rs_id;
                });
                $scope.replicaSet = newReplicaSet;
                $scope.node.selected = '';
                scope.listReplicaSet(function() {
                  $scope.availableNodes = scope.availableReceiver;
                  loadingRequest.hide();
                });
              }, function(e) {
                scope.listReplicaSet(function() {
                  $scope.availableNodes = scope.availableReceiver;
                  loadingRequest.hide();
                });
                errorMessage(e, 'Failed get replica set');
              });
            }, function(e) {
              scope.listReplicaSet(function() {
                loadingRequest.hide();
              });
              errorMessage(e, 'Failed to add node');
            });
          } else {
             errorMessage(e, 'Failed to add node');
          }
        };

        function errorMessage(e, errMsg) {
          $modalInstance.dismiss('cancel');
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : errMsg;
            SweetAlert.swal('Oops...', msg, 'error');
          } else {
            SweetAlert.swal('Oops...', errMsg, 'error');
          }
        };
      },
      windowClass: 'receiver-stats-modal',
      resolve: {
        replicaSet: function () {
          return replicaSet;
        },
        scope: function() {
          return $scope;
        },
        availableReceiver : function() {
          return $scope.availableReceiver;
        }
      }
    });
  };

  $scope.createReplicaSet = function() {
    $modal.open({
      templateUrl: 'createReplicaSet.html',
      controller: function ($scope, scope, $modalInstance, nodes, AdminStreamingService) {
        $scope.availableNodes = nodes;
        $scope.node = {
          selected: []
        };

        $scope.saveReplicaSet = function() {
          if ($scope.node.selected.length) {
            AdminStreamingService.createReplicaSet({}, {
              nodes: $scope.node.selected
            }, function(data) {
              scope.listReplicaSet();
              $modalInstance.close();
              SweetAlert.swal('Success!', 'Node add success', 'success');
            }, function(e) {
              if (e.data && e.data.exception) {
                var message = e.data.exception;
                var msg = !!(message) ? message : 'Failed to add node';
                SweetAlert.swal('Oops...', msg, 'error');
              } else {
                SweetAlert.swal('Oops...', 'Failed to add node', 'error');
              }
              scope.listReplicaSet();
            });
          } else {
            SweetAlert.swal('Oops...', "Please select node", 'info');
          }
        };

        $scope.cancel = function () {
          $modalInstance.dismiss('cancel');
        };

      },
      windowClass: 'receiver-stats-modal',
      resolve: {
        scope: function() {
          return $scope;
        },
        nodes: function() {
          var availableReceiver = [];
          angular.forEach($scope.availableReceiver, function(item) {
            availableReceiver.push(item.receiver);
          });
          return availableReceiver;
        }
      }
    });
  };

  $scope.removeReplicaSet = function(replicaSet) {
    SweetAlert.swal({
      title: '',
      text: 'Are you sure to delete replica set ['+replicaSet.rs_id+']? ',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function(isConfirm) {
      if(isConfirm){
        AdminStreamingService.removeReplicaSet({replicaSetId: replicaSet.rs_id}, {}, function (result) {
          $scope.listReplicaSet();
          SweetAlert.swal('Success!', 'Replica set remove success', 'success');
        }, function(e){
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Failed to remove replica set';
            SweetAlert.swal('Oops...', msg, 'error');
          } else {
            SweetAlert.swal('Oops...', 'Failed to remove replica set', 'error');
          }
          $scope.listReplicaSet();
        });
      }
    });
  };

  $scope.removeReceiver = function(receiverID) {
    SweetAlert.swal({
      title: '',
      text: 'Are you sure to remove receiver with id \'' + receiverID + '\'?',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function(isConfirm) {
      if(isConfirm){
        AdminStreamingService.removeReceiver({receiverID: receiverID}, {}, function (result) {
          SweetAlert.swal({title: 'Success!', text:'Receiver remove success'}, function (isConfirm) {
            if (isConfirm) {
              $timeout(function() {}, 2000);
              $scope.listReplicaSet();
            }
          });
        }, function(e){
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Failed to remove receiver';
            SweetAlert.swal('Oops...', msg, 'error');
          } else {
            SweetAlert.swal('Oops...', 'Failed to remove receiver', 'error');
          }
        });
      }
    });
  };

}).filter('formatId', function () {
  return function (item) {
    return item.split('.').join('_');
  }
});


KylinApp.controller('StreamingReceiverCtrl', function($scope, $routeParams, $modal, AdminStreamingService, MessageService, loadingRequest, UserService, ProjectModel){
  $scope.receiverId = $routeParams.receiverId;
  $scope.receiverServer = $scope.receiverId.split('_')[0];

  $scope.getReceiverStats = function() {
    AdminStreamingService.getReceiverStats({nodeId: $scope.receiverId}, function(data){
      $scope.receiverStats = data;
    }, function(e) {
      if (e.data && e.data.exception) {
        var message = e.data.exception;
        var msg = !!(message) ? message : 'Failed to get receiver stats';
        SweetAlert.swal('Oops...', msg, 'error');
      } else {
        SweetAlert.swal('Oops...', 'Failed to get receiver stats', 'error');
      }
    });
  }

  $scope.moreDetails = function(receiverCubeStats, cubeName) {
    $modal.open({
      templateUrl: 'receiverCubeDetails.html',
      controller: function ($scope, scope, $modalInstance, receiverCubeStats, cubeName, AdminStreamingService) {
        $scope.receiverCubeStats = receiverCubeStats;
        $scope.cubeName = cubeName;

        $scope.cancel = function () {
          $modalInstance.dismiss('cancel');
        };

      },
      resolve: {
        scope: function() {
          return $scope;
        },
        receiverCubeStats: function() {
          return receiverCubeStats;
        },
        cubeName: function() {
          return cubeName;
        }
      }
    });
  };

  $scope.getReceiverStats();

});
