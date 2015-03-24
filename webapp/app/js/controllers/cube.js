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

KylinApp.controller('CubeCtrl', function ($scope, AccessService, MessageService, CubeService, TableService, GraphService, UserService,SweetAlert,loadingRequest,ModelList,$modal) {
    $scope.newAccess = null;
    $scope.state = {jsonEdit: false};

    $scope.buildGraph = function (cube) {
       GraphService.buildTree(cube);
    };

    $scope.getCubeSql = function (cube) {
        CubeService.getSql({cubeId: cube.name, propValue: "null"}, function (sql) {
            cube.sql = sql.sql;
        },function(e){
            if(e.data&& e.data.exception){
                var message =e.data.exception;
                var msg = !!(message) ? message : 'Failed to take action.';
                SweetAlert.swal('Oops...', msg, 'error');
            }else{
                SweetAlert.swal('Oops...', "Failed to take action.", 'error');
            }
        });
    };

    $scope.getNotifyListString = function (cube) {
        if (cube.detail.notify_list) {
            cube.notifyListString = cube.detail.notify_list.join(",");
        }
        else {
            cube.notifyListString = "";
        }
    };

    $scope.cleanStatus = function(cube){

        if (!cube)
        {
            return;
        }
        var newCube = jQuery.extend(true, {}, cube);
        delete newCube.project;

        angular.forEach(newCube.dimensions, function(dimension, index){
            delete dimension.status;
        });

        return newCube;
    };

    $scope.updateNotifyList = function (cube) {
        cube.detail.notify_list = cube.notifyListString.split(",");
        CubeService.updateNotifyList({cubeId: cube.name}, cube.detail.notify_list, function () {
            SweetAlert.swal('Success!', 'Notify List updated successfully!', 'success');
        },function(e){
            if(e.data&& e.data.exception){
                var message =e.data.exception;
                var msg = !!(message) ? message : 'Failed to take action.';
                SweetAlert.swal('Oops...', msg, 'error');
            }else{
                SweetAlert.swal('Oops...', "Failed to take action.", 'error');
            }
        });
    };

    $scope.getHbaseInfo = function (cube) {
        if (!cube.hbase) {
            CubeService.getHbaseInfo({cubeId: cube.name, propValue: null, action: null}, function (hbase) {
                cube.hbase = hbase;

                // Calculate cube total size based on each htable.
                var totalSize = 0;
                hbase.forEach(function(t) {
                    totalSize += t.tableSize;
                });
                cube.totalSize = totalSize;
            },function(e){
                if(e.data&& e.data.exception){
                    var message =e.data.exception;
                    var msg = !!(message) ? message : 'Failed to take action.';
                    SweetAlert.swal('Oops...', msg, 'error');
                }else{
                    SweetAlert.swal('Oops...', "Failed to take action.", 'error');
                }
            });
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
//                    CubeList.removeCube(cube);
                    SweetAlert.swal('Success!', 'Cube drop is done successfully', 'success');

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
        $scope.metaModel.model = ModelList.getModelByCube(cube.name);
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
                    text: "Are you sure to start the build? ",
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
            $modal.open({
                templateUrl: 'jobRefresh.html',
                controller: jobSubmitCtrl,
                resolve: {
                    cube: function () {
                        return cube.detail;
                    },
                    metaModel:function(){
                        return $scope.metaModel;
                    },
                    buildType: function () {
                        return 'BUILD';
                    }
                }
            });

    };

    $scope.startMerge = function (cube) {
            $modal.open({
                templateUrl: 'jobMerge.html',
                controller: jobSubmitCtrl,
                resolve: {
                    cube: function () {
                        return cube.detail;
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


var jobSubmitCtrl = function ($scope, $modalInstance, CubeService, MessageService, $location, cube,metaModel, buildType,SweetAlert,loadingRequest) {
    $scope.cube = cube;
    $scope.metaModel={
        model:metaModel.model
    }
    $scope.jobBuildRequest = {
        buildType: buildType,
        startTime: 0,
        endTime: 0
    };
    $scope.message = "";

    $scope.rebuild = function (jobsubmit) {

        $scope.message = null;
        $scope.jobBuildRequest.startTime = new Date($scope.jobBuildRequest.startTime).getTime();
        $scope.jobBuildRequest.endTime = new Date($scope.jobBuildRequest.endTime).getTime();

        if ($scope.jobBuildRequest.startTime >= $scope.jobBuildRequest.endTime) {
            $scope.message = "WARNING: End time should be later than the start time.";

            //rollback date setting
            if(jobsubmit){
                $scope.rebuildRollback();
            }
            return;
        }

        loadingRequest.show();
        CubeService.rebuildCube({cubeId: cube.name}, $scope.jobBuildRequest, function (job) {

            loadingRequest.hide();
            $modalInstance.dismiss('cancel');
            SweetAlert.swal('Success!', 'Rebuild job was submitted successfully', 'success');
        },function(e){

            //rollback date setting
            if(jobsubmit){
                $scope.rebuildRollback();
            }

            loadingRequest.hide();
            if(e.data&& e.data.exception){
                var message =e.data.exception;
                var msg = !!(message) ? message : 'Failed to take action.';
                SweetAlert.swal('Oops...', msg, 'error');
            }else{
                SweetAlert.swal('Oops...', "Failed to take action.", 'error');
            }
        });
    };

    $scope.rebuildRollback = function(){
        $scope.jobBuildRequest.endTime+=new Date().getTimezoneOffset()*60000;
    }

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

    $scope.updateDate = function() {


//        if ($scope.cube.detail.partition_desc.cube_partition_type=='UPDATE_INSERT')
//        {
//            $scope.jobBuildRequest.startTime=$scope.formatDate($scope.jobBuildRequest.startTime);
//        }
        $scope.jobBuildRequest.endTime=$scope.formatDate($scope.jobBuildRequest.endTime);
    };

    $scope.formatDate = function(timestemp) {
        var dateStart = new Date(timestemp);
        dateStart = (dateStart.getFullYear() + "-" + (dateStart.getMonth() + 1) + "-" + dateStart.getDate());
        //switch selected time to utc timestamp
        return new Date(moment.utc(dateStart, "YYYY-MM-DD").format()).getTime();
    };
    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    };
};

