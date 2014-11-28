'use strict';

KylinApp
    .controller('CubesCtrl', function ($scope, $q, $routeParams, $location, $modal, MessageService, CubeDescService, CubeService, JobService, UserService,  ProjectService) {
        $scope.listParams={
            cubeName: $routeParams.cubeName,
            projectName: $routeParams.projectName
        };
        if($routeParams.projectName){
            $scope.project.selectedProject = $routeParams.projectName;
        }
        $scope.cubes = [];
        $scope.loading = false;
        $scope.action = {};

        $scope.theaditems = [
            {attr: 'name', name: 'Name'},
            {attr: 'status', name: 'Status'},
            {attr: 'size_kb', name: 'Cube Size'},
            {attr: 'source_records_count', name: 'Source Records'},
            {attr: 'last_build_time', name: 'Last Build Time'},
            {attr: 'owner', name: 'Owner'},
            {attr: 'create_time', name: 'Create Time'}
        ];

        $scope.state = { filterAttr: 'create_time', filterReverse: true, reverseColumn: 'create_time',
            dimensionFilter: '', measureFilter: ''};


        $scope.list = function (offset, limit) {
            offset = (!!offset) ? offset : 0;
            limit = (!!limit) ? limit : 20;
            var defer = $q.defer();

            var queryParam = {offset: offset, limit: limit};
            if ($scope.listParams.cubeName) {
                queryParam.cubeName = $scope.listParams.cubeName;
            }
            if ($scope.project.selectedProject){
                queryParam.projectName = $scope.project.selectedProject;
            }else{
                queryParam.projectName = $scope.project.projects[0];
            }

            $scope.loading = true;
            CubeService.list(queryParam, function (cubes) {
                angular.forEach(cubes, function (cube, index) {
                    $scope.listAccess(cube, 'CubeInstance');
                    if (cube.segments && cube.segments.length > 0) {
                        for(var i= cube.segments.length-1;i>=0;i--){
                            if(cube.segments[i].status==="READY"){
                                cube.last_build_time = cube.segments[i].last_build_time;
                                break;
                            }else if(i===0){
                                cube.last_build_time = cube.create_time;
                            }
                        }
                    } else {
                        cube.last_build_time = cube.create_time;
                    }
                    if($routeParams.showDetail == 'true'){
                        cube.showDetail = true;
                        $scope.loadDetail(cube);
                    }
                });
                $scope.cubes=[];
                $scope.cubes = $scope.cubes.concat(cubes);
                $scope.loading = false;
                defer.resolve(cubes.length);
            });

            return defer.promise;
        };

        $scope.$watch('project.selectedProject', function (newValue, oldValue) {
            //exclude when refresh page oldValue=null,first time set value for project (will have page auto reload ,incase duplicate) oldvalue is null
           if(oldValue){
                $scope.cubes=[];
                $scope.reload();
           }

        });
        $scope.reload = function () {
            // trigger reload action in pagination directive
            $scope.action.reload = !$scope.action.reload;
        };

        $scope.loadDetail = function (cube) {
            if (!cube.detail) {
                CubeDescService.get({cube_name: cube.name}, {}, function (detail) {
                    if (detail.length > 0) {
                        cube.detail = detail[0];
                    }
                }, function () {
                });
            }
        };

        $scope.getTotalSize = function (cubes) {
            var size = 0;
            if (!cubes) {
                return 0;
            }
            else {
                for(var i = 0; i < cubes.length; i++){
                    size += cubes[i].size_kb;
                }
                return $scope.dataSize(size*1024);
            }
        };

        $scope.enable = function (cube) {
            if (confirm("Are you sure to enable the cube? Please note: if cube schema is changed in the disabled period, all segments of the cube will be discarded due to data and schema mismatch.")) {
                CubeService.enable({cubeId: cube.name}, {}, function (result) {
                    cube.status = 'READY';
                    MessageService.sendMsg('Enable job was submitted successfully', 'success', {});
                });
            }
        };

        $scope.purge = function (cube) {
            if (confirm("Are you sure to purge the cube? ")) {
                CubeService.purge({cubeId: cube.name}, {}, function (result) {
                    $scope.cubes=[];
                    $scope.reload();
                    MessageService.sendMsg('Purge job was submitted successfully', 'success', {});
                });
            }
        }

        $scope.disable = function (cube) {
            if (confirm("Are you sure to disable the cube?")) {
                CubeService.disable({cubeId: cube.name}, {}, function (result) {
                    cube.status = 'DISABLED';
                    MessageService.sendMsg('Disable job was submitted successfully', 'success', {});
                });
            }
        };

        $scope.dropCube = function (cube) {
            if (confirm("Are you sure to drop the cube? Once it's dropped, all the jobs and data will be cleaned up.")) {
                CubeService.drop({cubeId: cube.name}, {}, function (result) {
                    var cubeIndex = $scope.cubes.indexOf(cube);
                    if (cubeIndex > -1) {
                        $scope.cubes.splice(cubeIndex, 1);
                    }
                    MessageService.sendMsg('Cube drop is done successfully', 'success', {});
                });
            }
        };

        $scope.startJobSubmit = function (cube) {
            CubeDescService.get({cube_name: cube.name}, {}, function (detail) {
                if (detail.length > 0) {
                    cube.detail = detail[0];
                    if (cube.detail.cube_partition_desc.partition_date_column) {
                        $modal.open({
                            templateUrl: 'jobSubmit.html',
                            controller: jobSubmitCtrl,
                            resolve: {
                                cube: function () {
                                    return cube;
                                },
                                buildType: function () {
                                    return 'BUILD';
                                }
                            }
                        });
                    }
                    else {
                        if (confirm("Are you sure to start the build?")) {
                            CubeService.rebuildCube(
                                {
                                    cubeId: cube.name
                                },
                                {
                                    buildType: 'BUILD',
                                    startTime: 0,
                                    endTime: 0
                                }, function (job) {
                                    MessageService.sendMsg('Rebuild job was submitted successfully', 'success', {
                                        cancel: {
                                            label: 'View Jobs',
                                            action: function () {
                                                $location.path('/jobs');
                                                $scope.$apply();
                                            }
                                        }
                                    });
                                });
                        }
                    }
                }
            });
        };

        $scope.startRefresh = function (cube) {
            $scope.loadDetail(cube);

            $modal.open({
                templateUrl: 'jobRefresh.html',
                controller: jobSubmitCtrl,
                resolve: {
                    cube: function () {
                        return cube;
                    },
                    buildType: function () {
                        return 'BUILD';
                    }
                }
            });
        };

        $scope.startMerge = function (cube) {
            $scope.loadDetail(cube);

            $modal.open({
                templateUrl: 'jobMerge.html',
                controller: jobSubmitCtrl,
                resolve: {
                    cube: function () {
                        return cube;
                    },
                    buildType: function () {
                        return 'MERGE';
                    }
                }
            });
        }
    });

var jobSubmitCtrl = function ($scope, $modalInstance, CubeService, MessageService, $location, cube, buildType) {
    $scope.cube = cube;

    $scope.jobBuildRequest = {
        buildType: buildType,
        startTime: 0,
        endTime: 0
    };
    $scope.message = "";

    $scope.rebuild = function () {
        if (confirm("Are you sure to rebuild the cube?")) {
            $scope.message = null;
            $scope.jobBuildRequest.startTime = new Date($scope.jobBuildRequest.startTime).getTime();
            $scope.jobBuildRequest.endTime = new Date($scope.jobBuildRequest.endTime).getTime();

            if ($scope.jobBuildRequest.startTime >= $scope.jobBuildRequest.endTime) {
                $scope.message = "WARNING: End time should be later than the start time.";
                return;
            }

            CubeService.rebuildCube({cubeId: cube.name}, $scope.jobBuildRequest, function (job) {
                $modalInstance.dismiss('cancel');
                MessageService.sendMsg('Rebuild job was submitted successfully', 'success', {
                    cancel: {
                        label: 'View Jobs',
                        action: function () {
                            $location.path('/jobs');
                            $scope.$apply();
                        }
                    }
                });
            });
        }
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

    $scope.updateDate = function() {
        if ($scope.cube.detail.cube_partition_desc.cube_partition_type=='UPDATE_INSERT')
        {
            $scope.jobBuildRequest.startTime=$scope.formatDate($scope.jobBuildRequest.startTime);
        }
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

