'use strict';

KylinApp
    .controller('JobCtrl', function ($scope, $q, $routeParams, $interval, $modal, ProjectService, MessageService, JobService) {
        $scope.cubeName = null;
        $scope.jobs = {};
        $scope.projects = [];
        $scope.action = {};
        $scope.allStatus = [
            {name: 'NEW', value: 0},
            {name: 'PENDING', value: 1},
            {name: 'RUNNING', value: 2},
            {name: 'FINISHED', value: 4},
            {name: 'ERROR', value: 8},
            {name: 'DISCARDED', value: 16}
        ];
        $scope.theaditems = [
            {attr: 'name', name: 'Job Name'},
            {attr: 'related_cube', name: 'Cube'},
            {attr: 'progress', name: 'Progress'},
            {attr: 'last_modified', name: 'Last Modified Time'},
            {attr: 'duration', name: 'Duration'}
        ];
        $scope.status = [];
        $scope.toggleSelection = function toggleSelection(current) {
            var idx = $scope.status.indexOf(current);
            if (idx > -1) {
              $scope.status.splice(idx, 1);
            }else {
              $scope.status.push(current);
            }
        };

        // projectName from page ctrl
        $scope.state = {loading: false, refreshing: false, filterAttr: 'last_modified', filterReverse: true, reverseColumn: 'last_modified', projectName:$scope.project.selectedProject};

        ProjectService.list({}, function (projects) {
            angular.forEach(projects, function(project, index){
                $scope.projects.push(project.name);
            });
        });

        $scope.list = function (offset, limit) {
            offset = (!!offset) ? offset : 0;
            var selectedJob = null;
            if (angular.isDefined($scope.state.selectedJob)) {
                selectedJob = $scope.state.selectedJob;
            }

            var defer = $q.defer();
            var statusIds = [];
            angular.forEach($scope.status, function (statusObj, index) {
                statusIds.push(statusObj.value);
            });

            var jobRequest = {
                cubeName: $scope.cubeName,
                projectName: $scope.state.projectName,
                status: statusIds,
                offset: offset,
                limit: limit
            };
            $scope.state.loading = true;
            JobService.list(jobRequest, function (jobs) {
                angular.forEach(jobs, function (job) {
                    var id = job.uuid;
                    if (angular.isDefined($scope.jobs[id])) {
                        if (job.last_modified != $scope.jobs[id].last_modified) {
                            $scope.jobs[id] = job;
                        } else {
                        }
                    } else {
                        $scope.jobs[id] = job;
                    }
                });

                $scope.state.loading = false;
                if (angular.isDefined($scope.state.selectedJob)) {
                    $scope.state.selectedJob = $scope.jobs[selectedJob.uuid];
                }
                defer.resolve(jobs.length);
            });

            return defer.promise;
        }

        $scope.reload = function () {
            // trigger reload action in pagination directive
            $scope.action.reload = !$scope.action.reload;
        };


        $scope.$watch('project.selectedProject', function (newValue, oldValue) {
            if(oldValue){
                $scope.jobs={};
                $scope.state.projectName = newValue;
                $scope.reload();
            }

        });
        $scope.resume = function (job) {
            if (confirm("Are you sure to resume the job?")) {
                JobService.resume({jobId: job.uuid}, {}, function (job) {
                    $scope.jobs[job.uuid] = job;
                    if (angular.isDefined($scope.state.selectedJob)) {
                        $scope.state.selectedJob = $scope.jobs[ $scope.state.selectedJob.uuid];
                    }
                    MessageService.sendMsg('Job was resumed successfully', 'success', {});
                });
            }
        }

        $scope.cancel = function (job) {
            if (confirm("Are you sure to discard the job?")) {
                JobService.cancel({jobId: job.uuid}, {}, function (job) {
                    $scope.jobs[job.uuid] = job;
                    if (angular.isDefined($scope.state.selectedJob)) {
                        $scope.state.selectedJob = $scope.jobs[ $scope.state.selectedJob.uuid];
                    }
                    MessageService.sendMsg('Job was cancelled successfully', 'success', {});
                });
            }
        }

        $scope.openModal = function () {
            if (angular.isDefined($scope.state.selectedStep)) {
                if ($scope.state.stepAttrToShow == "output") {
                    $scope.state.selectedStep.loadingOp = true;
                    internalOpenModal();
                    JobService.stepOutput({jobId: $scope.state.selectedJob.uuid, propValue: $scope.state.selectedStep.sequence_id}, function (result) {
                        if (angular.isDefined($scope.jobs[result['jobId']])) {
                            var tjob = $scope.jobs[result['jobId']];
                            tjob.steps[parseInt(result['stepId'])].cmd_output = result['cmd_output'];
                            tjob.steps[parseInt(result['stepId'])].loadingOp = false;
                        }
                    });
                } else {
                    internalOpenModal();
                }
            }
        }

        function internalOpenModal() {
            $modal.open({
                templateUrl: 'jobStepDetail.html',
                controller: jobStepDetail,
                resolve: {
                    step: function () {
                        return $scope.state.selectedStep;
                    },
                    attr: function () {
                        return $scope.state.stepAttrToShow;
                    }
                }
            });
        }
    }
);

var jobStepDetail = function ($scope, $modalInstance, step, attr) {
    $scope.step = step;
    $scope.stepAttrToShow = attr;
    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    }
};
