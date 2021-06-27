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

KylinApp.controller('CubesCtrl', function ($scope, $q, $routeParams, $location, $modal, MessageService, CubeDescService, CubeService, JobService, UserService, ProjectService, SweetAlert, loadingRequest, $log, cubeConfig, ProjectModel, ModelService, MetaModel, CubeList,modelsManager,TableService, kylinConfig, MessageBox, AdminStreamingService, tableConfig) {

    $scope.cubeConfig = cubeConfig;
    $scope.cubeList = CubeList;

    $scope.modelsManager = modelsManager;
    $scope.listParams = {
      cubeName: $routeParams.cubeName,
      projectName: $routeParams.projectName
    };
    if ($routeParams.projectName) {
      $scope.projectModel.setSelectedProject($routeParams.projectName);
    }
    CubeList.removeAll();
    $scope.loading = false;
    $scope.action = {};

    $scope.state = {
      filterAttr: 'create_time_utc', filterReverse: true, reverseColumn: 'create_time_utc',
      dimensionFilter: '', measureFilter: ''
    };

    $scope.refreshCube = function(cube){
      var queryParam = {
        cubeId: cube.name
      };
      var defer = $q.defer();
      CubeService.getCube(queryParam, function(newCube){
        var segmentsLen = newCube.segments && newCube.segments.length || 0
        newCube.input_records_count = 0;
        newCube.size_kb = 0;
        for(var i = segmentsLen - 1;i >= 0;i--){
          var curSeg = newCube.segments[i]
          if(curSeg.status === "READY"){
            newCube.input_records_count += curSeg.input_records
            newCube.size_kb += curSeg.size_kb
            if(newCube.last_build_time === undefined || newCube.last_build_time < curSeg.last_build_time) {
              newCube.last_build_time = curSeg.last_build_time;
            }
          }
        }
        newCube.project = cube.project;
        defer.resolve(newCube);
      },function(e){
        defer.resolve([]);
      })
      return defer.promise;
    }

    $scope.list = function (offset, limit) {
      var defer = $q.defer();
      if (!$scope.projectModel.projects.length) {
        defer.resolve([]);
        return defer.promise;
      }
      offset = (!!offset) ? offset : 0;
      limit = (!!limit) ? limit : 20;

      var queryParam = {offset: offset, limit: limit};
      if ($scope.listParams.cubeName) {
        queryParam.cubeName = $scope.listParams.cubeName;
      }
      queryParam.projectName = $scope.projectModel.selectedProject;

      $scope.loading = true;

      return CubeList.list(queryParam).then(function (resp) {
        angular.forEach($scope.cubeList.cubes,function(cube,index){
        })

        $scope.loading = false;
        defer.resolve(resp);
        return defer.promise;

      },function(resp){
        $scope.loading = false;
        defer.resolve([]);
        SweetAlert.swal('Oops...', resp, 'error');
        return defer.promise;
      });
    };

    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
      if (newValue != oldValue || newValue == null) {
        CubeList.removeAll();
        $scope.reload();
      }

    });
    $scope.reload = function () {
      // trigger reload action in pagination directive
      $scope.action.reload = !$scope.action.reload;
    };

    $scope.loadDetail = function (cube) {
      var defer = $q.defer();
      if (cube.detail) {
        defer.resolve(cube);
      } else {
        CubeDescService.query({cube_name: cube.name}, {}, function (detail) {
          if (detail.length > 0 && detail[0].hasOwnProperty("name")) {
            cube.detail = detail[0];
            cube.model = modelsManager.getModel(cube.detail.model_name);
              defer.resolve(cube);

          } else {
            SweetAlert.swal('Oops...', "No cube detail info loaded.", 'error');
          }
        }, function (e) {
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Failed to take action.';
            SweetAlert.swal('Oops...', msg, 'error');
          } else {
            SweetAlert.swal('Oops...', "Failed to take action.", 'error');
          }
        });
      }

      return defer.promise;
    };

    $scope.getStreamingInfo = function(cube) {
      var defer = $q.defer();
      if (cube.streamingV2) {
        defer.resolve(cube);
      } else {
        var cubeModel = modelsManager.getModel(cube.model.name);
        var cubeTable = cubeModel.fact_table;
        var cubeProject = $scope.projectModel.selectedProject || cubeModel.project;

        TableService.get({tableName: cubeTable, pro: cubeProject},function(table){
          if (table && table.source_type == 1) {
            cube.streaming = true;
          } else if (table && _.values(tableConfig.streamingSourceType).indexOf(table.source_type) > -1){
            cube.streamingV2 = true;
            if (table.source_type == tableConfig.streamingSourceType.kafka_hive) {
              cube.lambda = true;
            }
          }
          defer.resolve(cube);
        });
      }
      return defer.promise;
    };

    $scope.loadDetailWithStreamingV2Info = function(cube){
      $scope.loadDetail(cube).then(function(cube) {
        return $scope.getStreamingInfo(cube);
      });
    };

    $scope.getTotalSize = function (cubes) {
      var size = 0;
      if (!cubes) {
        return 0;
      }
      else {
        for (var i = 0; i < cubes.length; i++) {
          size += cubes[i].size_kb;
        }
        return $scope.dataSize(size * 1024);
      }
    };

    $scope.getStreamingV2State = function(cube) {
      var defer = $q.defer();
      if (cube.consumeState) {
        defer.resolve(cube);
      } else {
        if (cube.streamingV2) {
          AdminStreamingService.getConsumeState({cubeName:cube.name}, function(state) {
            cube.consumeState = state;
          });
        }
        defer.resolve(cube);
      }
      return defer.promise;
    };

    // List Cube Action
    $scope.listCubeAction = function(cube) {
      $scope.actionLoaded = false;
      $scope.loadDetail(cube).then(function(cube) {
        return $scope.getStreamingInfo(cube);
      }, function(reason) {
        console.error(reason);
        $scope.actionLoaded = true;
      }).then(function(cube) {
        return $scope.getStreamingV2State(cube);
      }, function(reason) {
        console.error(reason);
        $scope.actionLoaded = true;
      }).then(function(cube) {
        $scope.actionLoaded = true;
      });
    };

//    Cube Action
    $scope.enable = function (cube) {
      SweetAlert.swal({
        title: '',
        text: 'Are you sure to enable the cube? Please note: if cube schema is changed in the disabled period, all segments of the cube will be discarded due to data and schema mismatch.',
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){

          loadingRequest.show();
          CubeService.enable({cubeId: cube.name}, {}, function (result) {

            loadingRequest.hide();
            $scope.refreshCube(cube).then(function(_cube){
              if(_cube && _cube.name){
                $scope.cubeList.cubes[$scope.cubeList.cubes.indexOf(cube)] = _cube;
              }
            });
            MessageBox.successNotify('Enable job was submitted successfully');
          },function(e){

            loadingRequest.hide();
            if(e.data&& e.data.exception){
              var message =e.data.exception;
              var msg = !!(message) ? message : 'Failed to take action.';
              SweetAlert.swal('Oops...', msg, 'error');
            }else{
              SweetAlert.swal('Oops...', "Failed to take action.", 'error');
            }
          });
        }
      });
    };

    $scope.purge = function (cube) {
      SweetAlert.swal({
        title: '',
        text: 'Are you sure to purge the cube? ',
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "Yes",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){

          loadingRequest.show();
          CubeService.purge({cubeId: cube.name}, {}, function (result) {

            loadingRequest.hide();
            $scope.refreshCube(cube).then(function(_cube){
             if(_cube && _cube.name){
                $scope.cubeList.cubes[$scope.cubeList.cubes.indexOf(cube)] = _cube;
             }
            });
            MessageBox.successNotify('Purge job was submitted successfully');
          },function(e){
            loadingRequest.hide();
            if(e.data&& e.data.exception){
              var message =e.data.exception;
              var msg = !!(message) ? message : 'Failed to take action.';
              SweetAlert.swal('Oops...', msg, 'error');
            }else{
              SweetAlert.swal('Oops...', "Failed to take action.", 'error');
            }
          });
        }
      });
    }

    $scope.disable = function (cube) {

      SweetAlert.swal({
        title: '',
        text: 'Are you sure to disable the cube? ',
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "Yes",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){

          loadingRequest.show();
          CubeService.disable({cubeId: cube.name}, {}, function (result) {
            loadingRequest.hide();
            $scope.refreshCube(cube).then(function(_cube){
              if(_cube && _cube.name){
                $scope.cubeList.cubes[$scope.cubeList.cubes.indexOf(cube)] = _cube;
              }
            });
            MessageBox.successNotify('Disable job was submitted successfully');
          },function(e){

            loadingRequest.hide();
            if(e.data&& e.data.exception){
              var message =e.data.exception;
              var msg = !!(message) ? message : 'Failed to take action.';
              SweetAlert.swal('Oops...', msg, 'error');
            }else{
              SweetAlert.swal('Oops...', "Failed to take action.", 'error');
            }
          });
        }

      });
    };

    $scope.dropCube = function (cube) {

      SweetAlert.swal({
        title: '',
        text: " Once it's dropped, your cube’s metadata and data will be cleaned up and can’t be restored back. ",
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "Yes",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){

          loadingRequest.show();
          CubeService.drop({cubeId: cube.name}, {}, function (result) {
            loadingRequest.hide();
            MessageBox.successNotify('Cube drop is done successfully');
            $scope.cubeList.cubes.splice($scope.cubeList.cubes.indexOf(cube),1);
          },function(e){

            loadingRequest.hide();
            if(e.data&& e.data.exception){
              var message =e.data.exception;
              var msg = !!(message) ? message : 'Failed to take action.';
              SweetAlert.swal('Oops...', msg, 'error');
            }else{
              SweetAlert.swal('Oops...', "Failed to take action.", 'error');
            }
          });
        }

      });
    };

    $scope.isAutoMigrateCubeEnabled = function(){
      return kylinConfig.isAutoMigrateCubeEnabled();
    };

    $scope.migrateCube = function (cube) {
      SweetAlert.swal({
        title: '',
        text: "The cube will overwrite the same cube in prod env" +
        "\nMigrating cube will elapse a couple of minutes." +
        "\nPlease wait.",
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "Yes",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){
          loadingRequest.show();
          CubeService.autoMigrate({cubeId: cube.name, propName: $scope.projectModel.selectedProject}, {}, function (result) {
            loadingRequest.hide();
            MessageBox.successNotify(cube.name + ' migrate successfully!');
          },function(e){
            loadingRequest.hide();
            SweetAlert.swal('Migrate failed!', "Please contact your ADMIN.", 'error');
          });
        }
      });
    };

    $scope.startJobSubmit = function (cube) {

      $scope.metaModel={
        model:cube.model
      };

      if(cube.streaming){
        SweetAlert.swal({
          title: '',
          text: "Are you sure to start the build?",
          type: '',
          showCancelButton: true,
          confirmButtonColor: '#DD6B55',
          confirmButtonText: "Yes",
          closeOnConfirm: true
        }, function(isConfirm) {
          if(isConfirm){
            loadingRequest.show();
            CubeService.rebuildStreamingCube(
              {
                cubeId: cube.name
              },
              {
                sourceOffsetStart:0,
                sourceOffsetEnd:'9223372036854775807',
                buildType:'BUILD'
              }, function (job) {
                loadingRequest.hide();
                MessageBox.successNotify('Rebuild job was submitted successfully');
              },function(e){

                loadingRequest.hide();
                if(e.data&& e.data.exception){
                  var message =e.data.exception;
                  var msg = !!(message) ? message : 'Failed to take action.';
                  SweetAlert.swal('Oops...', msg, 'error');
                }else{
                  SweetAlert.swal('Oops...', "Failed to take action.", 'error');
                }
              });
          }
        })
        return;
      }

      //for batch cube build tip
      if ($scope.metaModel.model.name) {

        //for partition cube build tip
        if ($scope.metaModel.model.partition_desc.partition_date_column) {
          $modal.open({
            templateUrl: 'jobSubmit.html',
            controller: jobSubmitCtrl,
            resolve: {
              cube: function () {
                return cube;
              },
              metaModel:function(){
                return $scope.metaModel;
              },
              buildType: function () {
                return 'BUILD';
              },
              scope:function(){
                return $scope;
              }
            }
          });
        }

        //for not partition cube build tip
        else {
          SweetAlert.swal({
            title: '',
            text: "Are you sure to start the build ?",
            type: '',
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            confirmButtonText: "Yes",
            closeOnConfirm: true
          }, function(isConfirm) {
            if(isConfirm){

              loadingRequest.show();
              CubeService.rebuildCube(
                {
                  cubeId: cube.name
                },
                {
                  buildType: 'BUILD',
                  startTime: 0,
                  endTime: 0
                }, function (job) {

                  loadingRequest.hide();
                  MessageBox.successNotify('Rebuild job was submitted successfully');
                },function(e){

                  loadingRequest.hide();
                  if(e.data&& e.data.exception){
                    var message =e.data.exception;
                    var msg = !!(message) ? message : 'Failed to take action.';
                    SweetAlert.swal('Oops...', msg, 'error');
                  }else{
                    SweetAlert.swal('Oops...', "Failed to take action.", 'error');
                  }
                });
            }

          });
        }
      }

    };

    $scope.startRefresh = function (cube) {

      $scope.metaModel={
        model:cube.model
      };
      $modal.open({
        templateUrl: 'jobRefresh.html',
        controller: jobSubmitCtrl,
        resolve: {
          cube: function () {
            return cube;
          },
          metaModel:function(){
            return $scope.metaModel;
          },
          buildType: function () {
            return 'REFRESH';
          },
          scope:function(){
            return $scope;
          }
        }
      });
    };

    $scope.cloneCube = function(cube){
      if(!$scope.projectModel.selectedProject){
        SweetAlert.swal('Oops...', "Please choose project before clone.", 'info');
        return;
      }

      $modal.open({
        templateUrl: 'cubeClone.html',
        controller: cubeCloneCtrl,
        windowClass:"clone-cube-window",
        resolve: {
          cube: function () {
            return cube;
          }
        }
      });
    }
    $scope.cubeEdit = function (cube) {
      $location.path("cubes/edit/" + cube.name);
    }
    $scope.startMerge = function (cube) {

      $scope.metaModel={
        model:cube.model
      };
      $modal.open({
        templateUrl: 'jobMerge.html',
        controller: jobSubmitCtrl,
        resolve: {
          cube: function () {
            return cube;
          },
          metaModel:function(){
            return $scope.metaModel;
          },
          buildType: function () {
            return 'MERGE';
          },
          scope:function(){
            return $scope;
          }
        }
      });

    };

     $scope.startDeleteSegment = function (cube) {
       $scope.metaModel={
         model:modelsManager.getModelByCube(cube.name)
       };
       $modal.open({
         templateUrl: 'deleteSegment.html',
         controller: deleteSegmentCtrl,
         resolve: {
           cube: function () {
             return cube;
           },
           scope: function() {
             return $scope;
           }
         }
       });
     };

    $scope.startLookupRefresh = function(cube) {
      $scope.metaModel={
        model:cube.model
      };
      $modal.open({
        templateUrl: 'lookupRefresh.html',
        controller: lookupRefreshCtrl,
        resolve: {
          cube: function () {
            return cube;
          },
          scope:function(){
            return $scope;
          }
        }
      });
    };

    $scope.listCubeAccess = function (cube) {
      //check project auth for user
      $scope.cubeProjectEntity = _.find($scope.projectModel.projects, function(project) {return project.name == $scope.projectModel.selectedProject;});

      if (!!cube.uuid) {
        $scope.listAccess(cube, 'CubeInstance');
      }
    };

    // streaming cube action
    $scope.startCube = function(cube) {
      AdminStreamingService.assignCube({cubeName:cube.name}, {}, function(data){
        SweetAlert.swal({
          title: 'Success!',
          text: 'Cube start successful',
          type: 'success',
          confirmButtonText: 'OK',
          confirmButtonClass: 'btn-primary',
          closeOnConfirm: true
        }, function () {
          location.reload();
        });
      }, function(e){
        if(e.data&& e.data.exception){
          var message =e.data.exception;
          var msg = !!(message) ? message : 'Failed to start cube';
          SweetAlert.swal('Oops...', msg, 'error');
        } else{
          SweetAlert.swal('Oops...', 'Failed to start cube', 'error');
        }
      });
    };

    $scope.pauseCube = function(cube) {
      AdminStreamingService.suspendCubeConsume({cubeName:cube.name}, {}, function(data){
        SweetAlert.swal({
          title: 'Success!',
          text: 'Cube pause successful',
          type: 'success',
          confirmButtonText: 'OK',
          confirmButtonClass: 'btn-primary',
          closeOnConfirm: true
        }, function () {
          location.reload();
        });
      }, function(e){
        if(e.data&& e.data.exception){
          var message =e.data.exception;
          var msg = !!(message) ? message : 'Failed to pause cube';
          SweetAlert.swal('Oops...', msg, 'error');
        } else{
          SweetAlert.swal('Oops...', 'Failed to pause cube', 'error');
        }
      });
    };

    $scope.resumeCube = function(cube) {
      AdminStreamingService.resumeCubeConsume({cubeName:cube.name}, {}, function(data){
        SweetAlert.swal({
          title: 'Success!',
          text: 'Cube resume successful',
          type: 'success',
          confirmButtonText: 'OK',
          confirmButtonClass: 'btn-primary',
          closeOnConfirm: true
        }, function () {
          location.reload();
        });
      }, function(e){
        if(e.data&& e.data.exception){
          var message =e.data.exception;
          var msg = !!(message) ? message : 'Failed to resume cube';
          SweetAlert.swal('Oops...', msg, 'error');
        } else{
          SweetAlert.swal('Oops...', 'Failed to resume cube', 'error');
        }
      });
    };

    $scope.viewAssignment = function(cube) {
      AdminStreamingService.getCubeAssignment({cube: cube.name}, function(data) {
        $modal.open({
          templateUrl: 'cubeAssignment.html',
          controller: function($scope, assignment, cube, $modalInstance) {
            $scope.status = 'view';

            $scope.assignmentGridOptions = {
              paginationPageSize: 20,
              columnDefs: [
                { name: 'Replica Set ID', field: 'rs_id', width:'20%'},
                { name: 'Partition', field: 'partitions', width:'*', cellTemplate: '<div class="ui-grid-cell-contents"><span class="label label-primary" style="margin-right:5px;" ng-repeat="partition in row.entity.partitions">{{partition.partition_id}}</span></div>' }
              ]
            };
            $scope.assignmentGridOptions.data = assignment[cube];

            $scope.cancel = function() {
              $modalInstance.dismiss('cancel');
            };

            $scope.editAssignment = function() {
              $scope.cubePartitions = [];
              angular.forEach(assignment[cube], function(replicaSet, index) {
                $scope.cubePartitions = $scope.cubePartitions.concat(replicaSet.partitions);
              });
              $scope.replicaSetIds = [];

              AdminStreamingService.getReplicaSets({}, function (data) {
                angular.forEach(data, function(rs, index) {
                  $scope.replicaSetIds.push(rs.rs_id);
                });
                $scope.status = 'edit';
                $scope.currentAssignment = assignment[cube];
              },function(e){
                if (e.data && e.data.exception) {
                  var message = e.data.exception;
                  var msg = !!(message) ? message : 'Failed to get replica set info';
                  SweetAlert.swal('Oops...', msg, 'error');
                } else {
                  SweetAlert.swal('Oops...', 'Failed to get replica set info', 'error');
                }
              });
            };

            $scope.removeReplicaSet = function(index) {
              console.log('remove element:', $scope.currentAssignment[index]);
              $scope.currentAssignment.splice(index, 1);
            };

            $scope.addReplicaSet = function() {
              $scope.currentAssignment.push({rs_id:"", partitions:[]});
            };

            $scope.reAssignCube = function() {
              AdminStreamingService.reAssignCube({cubeName: cube}, {cube_name: cube, assignments: $scope.currentAssignment}, function(data){
                $scope.cancel();
              }, function(e){
                if(e.data&& e.data.exception){
                  var message =e.data.exception;
                  var msg = !!(message) ? message : 'Failed to reAssign cube';
                  SweetAlert.swal('Oops...', msg, 'error');
                } else{
                  SweetAlert.swal('Oops...', 'Failed to reAssign cube', 'error');
                }
              });
            };
          },
          resolve: {
            assignment: function() {
              return data;
            },
            cube: function() {
              return cube.name;
            }
          }
        });
      }, function(e) {
        if(e.data&& e.data.exception){
          var message =e.data.exception;
          var msg = !!(message) ? message : 'Failed to get cube assignment info';
          SweetAlert.swal('Oops...', msg, 'error');
        } else{
          SweetAlert.swal('Oops...', 'Failed to get cube assignment info', 'error');
        }
      });
    };

  });


var cubeCloneCtrl = function ($scope, $modalInstance, CubeService, MessageService, $location, cube, MetaModel, SweetAlert,ProjectModel, loadingRequest, MessageBox) {
  $scope.projectModel = ProjectModel;

  $scope.targetObj={
    cubeName:cube.descriptor+"_clone",
    targetProject:$scope.projectModel.selectedProject
  }

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.cloneCube = function(){

    if(!$scope.targetObj.targetProject){
      SweetAlert.swal('Oops...', "Please select target project.", 'info');
      return;
    }

    $scope.cubeRequest = {
      cubeName:$scope.targetObj.cubeName,
      project:$scope.targetObj.targetProject
    }

    SweetAlert.swal({
      title: '',
      text: 'Are you sure to clone the cube? ',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {

        loadingRequest.show();
        CubeService.clone({cubeId: cube.name}, $scope.cubeRequest, function (result) {
          loadingRequest.hide();
          MessageBox.successNotify('Clone cube successfull');
          location.reload();
        }, function (e) {
          loadingRequest.hide();
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Failed to take action.';
            SweetAlert.swal('Oops...', msg, 'error');
          } else {
            SweetAlert.swal('Oops...', "Failed to take action.", 'error');
          }
        });
      }
    });
  }

}


var jobSubmitCtrl = function ($scope, $modalInstance, CubeService, MessageService, $location, cube, metaModel, buildType, SweetAlert, loadingRequest, scope, CubeList,$filter, MessageBox) {
  $scope.cubeList = CubeList;
  $scope.cube = cube;
  $scope.metaModel = metaModel;
  $scope.jobBuildRequest = {
    buildType: buildType,
    startTime: 0,
    endTime: 0
  };
  $scope.message = "";
  $scope.refreshType = 'normal';
  var startTime;
  if(cube.segments.length == 0){
    startTime = (!!cube.detail.partition_date_start)?cube.detail.partition_date_start:0;
  }else{
    startTime = cube.segments[cube.segments.length-1].date_range_end;
  }
  $scope.jobBuildRequest.startTime=startTime;
  $scope.rebuild = function (isForce) {
    $scope.message = null;
    if ($scope.jobBuildRequest.startTime >= $scope.jobBuildRequest.endTime) {
      $scope.message = "WARNING: End time should be later than the start time.";
      return;
    }
    $scope.jobBuildRequest.forceMergeEmptySegment = !!isForce;
    loadingRequest.show();
    CubeService.rebuildCube({cubeId: cube.name}, $scope.jobBuildRequest, function (job) {
      loadingRequest.hide();
      $modalInstance.dismiss('cancel');
      MessageBox.successNotify('Rebuild job was submitted successfully');
      scope.refreshCube(cube).then(function(_cube){
          $scope.cubeList.cubes[$scope.cubeList.cubes.indexOf(cube)] = _cube;
        });
    }, function (e) {
      loadingRequest.hide();
      if (e.data && e.data.exception) {
        var message = e.data.exception;

        if(message.indexOf("Empty cube segment found")!=-1){
          var _segment = message.substring(message.indexOf(":")+1,message.length-1);
          SweetAlert.swal({
            title:'',
            type:'info',
            text: 'Empty cube segment found'+':'+_segment+', do you want to merge segments forcely ?',
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            closeOnConfirm: true
          }, function (isConfirm) {
            if (isConfirm) {
              $scope.rebuild(true);
            }
          });
          return;
        }
        if(message.indexOf("Merging segments must not have gaps between")!=-1){
          SweetAlert.swal({
            title:'',
            type:'info',
            text: 'There ares gaps between segments, do you want to merge segments forcely ?',
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            closeOnConfirm: true
          }, function (isConfirm) {
            if (isConfirm) {
              $scope.rebuild(true);
            }
          });
          return;
        }
        var msg = !!(message) ? message : 'Failed to take action.';
        SweetAlert.swal('Oops...', msg, 'error');
      } else {
        SweetAlert.swal('Oops...', "Failed to take action.", 'error');
      }
    });
  };

  // used by cube segment refresh
  $scope.segmentSelected = function (selectedSegment) {
    $scope.jobBuildRequest.startTime = 0;
    $scope.jobBuildRequest.endTime = 0;

    if (selectedSegment.date_range_start) {
      $scope.jobBuildRequest.startTime = selectedSegment.date_range_start;
    }

    if (selectedSegment.date_range_end) {
      $scope.jobBuildRequest.endTime = selectedSegment.date_range_end;
    }
  };

  // used by cube segments merge
  $scope.mergeStartSelected = function (mergeStartSeg) {
    $scope.jobBuildRequest.startTime = 0;

    if (mergeStartSeg.date_range_start) {
      $scope.jobBuildRequest.startTime = mergeStartSeg.date_range_start;
    }
  };

  $scope.mergeEndSelected = function (mergeEndSeg) {
    $scope.jobBuildRequest.endTime = 0;

    if (mergeEndSeg.date_range_end) {
      $scope.jobBuildRequest.endTime = mergeEndSeg.date_range_end;
    }
  };

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.getAdvRefreshTimeOptions = function(status) {
    if ('start' === status) {
      var startTimeOptions = [];
      var lastInd = $scope.cube.segments.length - 1;
      angular.forEach($scope.cube.segments, function(segment, ind) {
        startTimeOptions.push(segment.date_range_start);
        if (lastInd == ind) {
          startTimeOptions.push(segment.date_range_end);
        }
      });
      return startTimeOptions;
    } else if ('end' === status) {
      var endTimeOptions = [];
      angular.forEach($scope.cube.segments, function(segment, ind) {
        endTimeOptions.push(segment.date_range_end);
      });
      return endTimeOptions;
    }
  };
  $scope.advRefreshStartTimeOptions = $scope.getAdvRefreshTimeOptions('start');
  $scope.advRefreshEndTimeOptions = $scope.getAdvRefreshTimeOptions('end');
  $scope.endTimeTypeCustomize = false;

  $scope.changeEndTimeDisplay = function() {
    $scope.endTimeTypeCustomize = !$scope.endTimeTypeCustomize;
  };

  $scope.setDateRange = function($view, $dates) {
    var minDate = $scope.cube.segments[$scope.cube.segments.length-1].date_range_end;
    // var maxDate = moment().startOf($view).valueOf(); // Now
    angular.forEach($dates, function(date) {
      var utcDateValue = date.utcDateValue;
      date.selectable = utcDateValue >= minDate; // && utcDateValue <= maxDate;
    });
  };

  $scope.changeRefreshType = function (type) {
    $scope.refreshType = type;
    if (type ==='normal') {
      $scope.jobBuildRequest.buildType = 'REFRESH';
    } else if (type === 'advance'){
      $scope.jobBuildRequest.buildType = 'BUILD';
    }
  }
};


var streamingBuildCtrl = function ($scope, $modalInstance,kylinConfig) {
  $scope.kylinConfig = kylinConfig;
  var streamingGuildeUrl = kylinConfig.getProperty("kylin.web.link-streaming-guide");
  $scope.streamingBuildUrl = streamingGuildeUrl?streamingGuildeUrl:"http://kylin.apache.org/";

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };
};

var deleteSegmentCtrl = function($scope, $modalInstance, CubeService, SweetAlert, loadingRequest, cube, scope, MessageBox) {
  $scope.cube = cube;
  $scope.deleteSegments = [];
  $scope.segment = {};

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.deleteSegment = function() {
    SweetAlert.swal({
      title: '',
      text: 'Are you sure to delete segment ['+$scope.segment.selected.name+']? ',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function(isConfirm) {
      if(isConfirm){
        loadingRequest.show();
        CubeService.deleteSegment({cubeId: cube.name, propValue: $scope.segment.selected.name}, {}, function (result) {
          loadingRequest.hide();
          $modalInstance.dismiss('cancel');
          scope.refreshCube(cube).then(function(_cube){
           if(_cube && _cube.name){
              scope.cubeList.cubes[scope.cubeList.cubes.indexOf(cube)] = _cube;
           }
          });
          MessageBox.successNotify('Delete segment successfully');
        },function(e){
          loadingRequest.hide();
          if(e.data&& e.data.exception){
            var message =e.data.exception;
            var msg = !!(message) ? message : 'Failed to delete segment.';
            SweetAlert.swal('Oops...', msg, 'error');
          }else{
            SweetAlert.swal('Oops...', 'Failed to delete segment.', 'error');
          }
        });
      }
    });
  };
};

var lookupRefreshCtrl = function($scope, scope, CubeList, $modalInstance, CubeService, cube, SweetAlert, loadingRequest, MessageBox) {
  $scope.cubeList = CubeList;
  $scope.cube = cube;
  $scope.dispalySegment = false;

  $scope.getLookups = function() {
    var modelLookups = cube.model ? cube.model.lookups : [];
    var cubeLookups = [];
    angular.forEach(modelLookups, function(modelLookup, index) {
      var dimensionTables = _.find(cube.detail.dimensions, function(dimension){ return dimension.table === modelLookup.alias;});
      if (!!dimensionTables) {
        if (cubeLookups.indexOf(modelLookup.table) === -1) {
          cubeLookups.push(modelLookup.table);
        }
      }
    });
    return cubeLookups;
  };

  $scope.cubeLookups = $scope.getLookups();

  $scope.lookup = {
    select: {}
  };

  $scope.getReadySegment = function(segment) {
    return segment.status === 'READY';
  };

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.updateLookupTable = function(tableName) {
    var lookupTable = _.find(cube.detail.snapshot_table_desc_list, function(table){ return table.table_name == tableName});
    if (!!lookupTable && lookupTable.global) {
      $scope.dispalySegment = false;
      $scope.lookup.select.segments = [];
    } else {
      $scope.dispalySegment = true;
    }
  };

  $scope.selectAllSegments = function(allSegments) {
    if (allSegments) {
      $scope.lookup.select.segments = $scope.cube.segments;
    } else {
      $scope.lookup.select.segments = [];
    }
  };

  $scope.refresh = function() {
    if (!$scope.lookup.select.table_name) {
      SweetAlert.swal('Warning', 'Lookup table should not be empty', 'warning');
      return;
    }

    // cube advance lookup table
    var lookupTable = _.find(cube.detail.snapshot_table_desc_list, function(table){ return table.table_name == $scope.lookup.select.table_name});
    if (!!lookupTable) {
      if (!lookupTable.global && $scope.lookup.select.segments.length == 0) {
        SweetAlert.swal('Warning', 'Segment should not be empty', 'warning');
        return;
      }
    } else {
      // cube lookup table
      lookupTable = _.find($scope.cubeLookups, function(table){ return table == $scope.lookup.select.table_name});
      if (!lookupTable) {
        SweetAlert.swal('Warning', 'Lookup table not existed in cube', 'warning');
        return;
      } else {
        if (!$scope.lookup.select.segments || $scope.lookup.select.segments.length == 0) {
          SweetAlert.swal('Warning', 'Segment should not be empty', 'warning');
          return;
        }
      }
    }

    var lookupSnapshotBuildRequest = {
      lookupTableName: $scope.lookup.select.table_name,
      segmentIDs: _.map($scope.lookup.select.segments, function(segment){ return segment.uuid})
    };

    loadingRequest.show();
    CubeService.lookupRefresh({cubeId: cube.name}, lookupSnapshotBuildRequest, function (job) {
      loadingRequest.hide();
      $modalInstance.dismiss('cancel');
      MessageBox.successNotify('Lookup refresh job was submitted successfully');
      scope.refreshCube(cube).then(function(_cube){
          $scope.cubeList.cubes[$scope.cubeList.cubes.indexOf(cube)] = _cube;
        });
    }, function (e) {
       loadingRequest.hide();
      if (e.data && e.data.exception) {
        var message = e.data.exception;

        var msg = !!(message) ? message : 'Failed to take action.';
        SweetAlert.swal('Oops...', msg, 'error');
      } else {
        SweetAlert.swal('Oops...', "Failed to take action.", 'error');
      }
    });
  };

};
