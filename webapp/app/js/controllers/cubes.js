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

KylinApp
  .controller('CubesCtrl', function ($scope, $q, $routeParams, $location, $modal, MessageService, CubeDescService, CubeService, JobService, UserService, ProjectService, SweetAlert, loadingRequest, $log, cubeConfig, ProjectModel, ModelService, MetaModel, CubeList,modelsManager,cubesManager,StreamingList,kylinConfig) {

    $scope.cubeConfig = cubeConfig;
    $scope.cubeList = CubeList;

    $scope.modelsManager = modelsManager;
    //$scope.cubesManager = cubesManager;

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
      filterAttr: 'create_time', filterReverse: true, reverseColumn: 'create_time',
      dimensionFilter: '', measureFilter: ''
    };

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

      //resolve cubes length
      return CubeList.list(queryParam).then(function (resp) {

        StreamingList.list().then(function(_resp){
          angular.forEach($scope.cubeList.cubes,function(item,index){
            var result = StreamingList.checkCubeExist(item.name);
            if(result.exist == true){
              item.streaming = result.streaming;
              var kfkConfig = StreamingList.getKafkaConfig(result.streaming.name);
              item.kfkConfig = kfkConfig;
            }
          })
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
        defer.resolve(cube.detail);
      } else {
        CubeDescService.query({cube_name: cube.name}, {}, function (detail) {
          if (detail.length > 0 && detail[0].hasOwnProperty("name")) {
            cube.detail = detail[0];
            cube.model = modelsManager.getModel(cube.detail.model_name);
              defer.resolve(cube.detail);

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

//    Cube Action
    $scope.enable = function (cube) {
      SweetAlert.swal({
        title: '',
        text: 'Are you sure to enable the cube? Please note: if cube schema is changed in the disabled period, all segments of the cube will be discarded due to data and schema mismatch.',
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
//                confirmButtonText: "Yes",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){

          loadingRequest.show();
          CubeService.enable({cubeId: cube.name}, {}, function (result) {

            loadingRequest.hide();
            cube.status = 'READY';
            SweetAlert.swal('Success!', 'Enable job was submitted successfully', 'success');
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
//                    CubeList.removeAll();
//                    $scope.reload();
            SweetAlert.swal('Success!', 'Purge job was submitted successfully', 'success');
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
            cube.status = 'DISABLED';
            SweetAlert.swal('Success!', 'Disable job was submitted successfully', 'success');
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
            SweetAlert.swal('Success!', 'Cube drop is done successfully', 'success');
            location.reload();
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

    $scope.startJobSubmit = function (cube) {
      $scope.loadDetail(cube);
      // for streaming cube build tip
      if(cube.streaming){
        $modal.open({
          templateUrl: 'streamingBuild.html',
          controller: streamingBuildCtrl,
          resolve: {
          }
        });
        return;
      }

      $scope.metaModel={
        model:modelsManager.getModelByCube(cube.name)
      }
      if ($scope.metaModel.model.name) {
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
              }
            }
          });
        }
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
                  SweetAlert.swal('Success!', 'Rebuild job was submitted successfully', 'success');
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
        model:modelsManager.getModelByCube(cube.name)
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
          }
        }
      });

    };

    $scope.cubeEdit = function (cube) {
      $location.path("cubes/edit/" + cube.name);
    }
    $scope.startMerge = function (cube) {
      $scope.metaModel={
        model:modelsManager.getModelByCube(cube.name)
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
          }
        }
      });
    }
  });

var jobSubmitCtrl = function ($scope, $modalInstance, CubeService, MessageService, $location, cube, metaModel, buildType, SweetAlert, loadingRequest) {
  $scope.cube = cube;
  $scope.metaModel = metaModel;
  $scope.jobBuildRequest = {
    buildType: buildType,
    startTime: 0,
    endTime: 0
  };
  $scope.message = "";

  $scope.rebuild = function () {

    $scope.message = null;

    if ($scope.jobBuildRequest.startTime >= $scope.jobBuildRequest.endTime) {
      $scope.message = "WARNING: End time should be later than the start time.";

      return;
    }

    loadingRequest.show();
    CubeService.rebuildCube({cubeId: cube.name}, $scope.jobBuildRequest, function (job) {

      loadingRequest.hide();
      $modalInstance.dismiss('cancel');
      SweetAlert.swal('Success!', 'Rebuild job was submitted successfully', 'success');
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
              $scope.jobBuildRequest.forceMergeEmptySegment = true;
              $scope.rebuild();
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
};


var streamingBuildCtrl = function ($scope, $modalInstance,kylinConfig) {
  $scope.kylinConfig = kylinConfig;
  var streamingGuildeUrl = kylinConfig.getProperty("kylin.web.streaming.guide");
  $scope.streamingBuildUrl = streamingGuildeUrl?streamingGuildeUrl:"http://kylin.apache.org/";

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };
}
