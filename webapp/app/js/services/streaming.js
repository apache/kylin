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

KylinApp.factory('StreamingService', ['$resource', function ($resource, config) {
    return $resource(Config.service.url + 'streaming/:streamingId/:propName/:propValue/:action', {}, {
        list: {method: 'GET', params: {}, isArray: true},
        'getConfig': {method: 'GET',params: {action:'getConfig'},isArray:true},
        'getKfkConfig': {method: 'GET',params: {action:'getKfkConfig'},isArray:true},
        drop: {method: 'DELETE', params: {}, isArray: false},
        save: {method: 'POST', params: {}, isArray: false},
        update: {method: 'PUT', params: {}, isArray: false}
    });
}]);

KylinApp.factory('StreamingServiceV2', ['$resource', function ($resource, config) {
    return $resource(Config.service.url + 'streaming_v2/:streamingId/:propName/:propValue/:action', {}, {
        'getConfig': {method: 'GET',params: {action:'getConfig'},isArray:true},
        'getParserTemplate': {method: 'GET', params: {propName:'parserTemplate'}, isArray: false},
        save: {method: 'POST', params: {}, isArray: false},
        update: {method: 'PUT', params: {action: 'updateConfig'}, isArray: false}
    });
}]);

KylinApp.factory('AdminStreamingService', ['$resource', function ($resource, config) {
  return $resource(Config.service.url + 'streaming_v2/:propName/:replicaSetId/:receiverID/:cubeName/:nodeId/:action', {}, {
    getReplicaSets: {
      method: 'GET',
      params: {
        propName: 'replicaSets'
      },
      isArray: true,
      interceptor: {
        response: function(response) {
          return response.data;
        }
      }
    },
    getClusterState: {
      method: 'GET',
      params: {
        propName: 'cluster',
        action: 'state'
      },
      isArray: false,
      interceptor: {
        response : function(response) {
          var clusterState = response.data;
          clusterState.rs_states.forEach(function(receiverState, rs_ind) {
            var assignment = '';
            for (var cube in receiverState.assignment) {
              assignment += '<br>' + cube;
            }
            receiverState.assignmentStr = assignment.substr(4);
          });
          return clusterState;
        }
      }
    },
    getReceiverStats:  {
      method: 'GET',
      params: {
        propName: 'receivers',
        action: 'stats'
      },
      isArray: false
    },
    getNodes: {
      method: 'GET',
      params: {
        propName: 'receivers'
      },
      isArray: true
    },
    addNodeToReplicaSet: {
      method: 'PUT',
      params: {
        propName: 'replicaSet'
      },
      isArray: false
    },
    removeNodeToReplicaSet: {
      method: 'DELETE',
      params: {
        propName: 'replicaSet'
      },
      isArray: false
    },
    createReplicaSet: {
      method: 'POST',
      params: {
        propName: 'replicaSet'
      },
      isArray: false
    },
    removeReplicaSet: {
      method: 'DELETE',
      params: {
        propName: 'replicaSet'
      },
      isArray: false
    },
    removeReceiver: {
      method: 'DELETE',
      params: {
        propName: 'receivers'
      },
      isArray: false
    },
    getBalanceRecommend: {
      method: 'GET',
      params: {
        propName: 'balance',
        action: 'recommend'
      },
      isArray: false,
      interceptor: {
        response: function(response) {
          var balancePlan = [];
          var data = JSON.parse(angular.toJson(response.data));
          for (var rs_id in data) {
            var orgCubeInfo = data[rs_id];
            var replicaSet = {
              rs_id: rs_id,
              cubeInfos: []
            }
            for(var cubeName in orgCubeInfo) {
              var cubeInfo = {
                cubeName: cubeName,
                partitions: []
              };
              angular.forEach(orgCubeInfo[cubeName], function(partition) {
                 cubeInfo.partitions.push({partitionId: partition.partition_id});
              });
              replicaSet.cubeInfos.push(cubeInfo);
            }
            replicaSet.cubeInfos = _.sortBy(replicaSet.cubeInfos, 'cubeName');
            balancePlan.push(replicaSet);
          }
          return balancePlan;
        }
      }
    },
    reBalance: {
      method: 'POST',
      params: {
        propName: 'balance'
      },
      isArray: false,
      transformRequest: function(replicaSets) {
        var balancePlan = {};
        angular.forEach(replicaSets.reBalancePlan, function(replicaSet) {
          balancePlan[replicaSet.rs_id] = {};
          angular.forEach(replicaSet.cubeInfos, function(cubeInfo) {
            var partitionArr = [];
            angular.forEach(cubeInfo.partitions, function(partion) {
              partitionArr.push({partition_id: parseInt(partion.partitionId)});
            });
            balancePlan[replicaSet.rs_id][cubeInfo.cubeName] = partitionArr;
          });
        });
        return JSON.stringify(balancePlan);
      }
    },
    assignCube: {
      method: 'PUT',
      params: {
        propName: 'cubes',
        action: 'assign'
      },
      isArray: false
    },
    suspendCubeConsume: {
      method: 'PUT',
      params: {
        propName: 'cubes',
        action: 'suspendConsume'
      },
      isArray: false
    },
    resumeCubeConsume: {
      method: 'PUT',
      params: {
        propName: 'cubes',
        action: 'resumeConsume'
      },
      isArray: false
    },
    getCubeAssignment: {
      method: 'GET',
      params: {
        propName: 'cubeAssignments'
      },
      isArray: true,
      interceptor: {
        response: function(response) {
          var assignments = {};
          angular.forEach(response.data, function(assignment, ind) {
            var cubeName = assignment.cube_name;
            assignments[cubeName] = [];
            for (var rs_id in assignment.assignments) {
              var assignmentInfo = {
                rs_id: rs_id,
                partitions: assignment.assignments[rs_id]
              };
              assignments[cubeName].push(assignmentInfo);
            }
          });
          return assignments;
        }
      }
    },
    getCubeRealTimeStats: {
      method: 'GET',
      params: {
        propName: 'cubes',
        action: 'stats'
      },
      isArray: false,
      interceptor: {
        response: function(response) {
          var data = JSON.parse(angular.toJson(response.data));
          for (var rsId in data.receiver_cube_real_time_states) {
            var receiverCount = Object.keys(data.receiver_cube_real_time_states[rsId]).length;
            for (var node in data.receiver_cube_real_time_states[rsId]) {
              var receiver_cube_real_time_states = data.receiver_cube_real_time_states[rsId][node]['receiver_cube_stats'];
              if (receiver_cube_real_time_states) {
                receiver_cube_real_time_states.consumer_info = {
                  avg_rate: 0,
                  one_min_rate: 0,
                  five_min_rate: 0,
                  fifteen_min_rate: 0,
                  total_consume: 0,
                };
                if (receiver_cube_real_time_states.consumer_stats) {
                  var offsetInfo = JSON.parse(receiver_cube_real_time_states.consumer_stats.consume_offset_info);
                  for (var partitionId in receiver_cube_real_time_states.consumer_stats.partition_consume_stats) {
                    var partitonInfo = receiver_cube_real_time_states.consumer_stats.partition_consume_stats[partitionId];
                    partitonInfo.offset_info = offsetInfo[partitionId];
                    partitonInfo.partition_id = partitionId;
                    receiver_cube_real_time_states.consumer_info.avg_rate += partitonInfo.avg_rate;
                    receiver_cube_real_time_states.consumer_info.one_min_rate += partitonInfo.one_min_rate;
                    receiver_cube_real_time_states.consumer_info.five_min_rate += partitonInfo.five_min_rate;
                    receiver_cube_real_time_states.consumer_info.fifteen_min_rate += partitonInfo.fifteen_min_rate;
                    receiver_cube_real_time_states.consumer_info.total_consume += partitonInfo.total_consume;
                  }
                  receiver_cube_real_time_states.partition_consume_stats = receiver_cube_real_time_states.consumer_stats.partition_consume_stats;
                  receiver_cube_real_time_states.consume_lag = receiver_cube_real_time_states.consumer_stats.consume_lag;
                }
                delete receiver_cube_real_time_states.consumer_stats;
                receiver_cube_real_time_states.receiver_state =  data.receiver_cube_real_time_states[rsId][node]['receiver_state'];
                data.receiver_cube_real_time_states[rsId][node] = receiver_cube_real_time_states;
              }
              data.receiver_cube_real_time_states[rsId][node]['style'] = 'col-sm-' + Math.floor(12/receiverCount);
            }
          }
          return data;
        }
      }
    },
    reAssignCube: {
      method: 'POST',
      params: {
        propName: 'cubes',
        action: 'reAssign'
      },
      isArray: false,
      transformRequest: function(newAssignment) {
        var transformAssignment = {};
        transformAssignment.cube_name = newAssignment.cube_name;
        transformAssignment.assignments = {};
        angular.forEach(newAssignment.assignments, function(assignment) {
          transformAssignment.assignments[assignment.rs_id] = assignment.partitions;
        });
        return angular.toJson(transformAssignment);
      }
    },
    getConsumeState: {
      method: 'GET',
      params: {
        propName: 'cubes',
        action: 'consumeState'
      },
      isArray: false,
      interceptor: {
        response: function(response){
          return response.data;
        }
      }
    }
  });
}]);
