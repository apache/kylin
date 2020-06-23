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
    .controller('JobCtrl', function ($scope, $q, $routeParams, $interval, $modal, ProjectService, MessageService, JobService,SweetAlert,loadingRequest,UserService,jobConfig,JobList,$window, MessageBox) {

        $scope.jobList = JobList;
        JobList.removeAll();
        $scope.jobConfig = jobConfig;
        $scope.cubeName = JobList.jobFilter.cubeName;
        //$scope.projects = [];
        $scope.action = {};
        $scope.timeFilter = jobConfig.timeFilter[JobList.jobFilter.timeFilterId];
        $scope.searchMode = jobConfig.searchMode[JobList.jobFilter.searchModeId];
        if ($routeParams.jobTimeFilter) {
            $scope.timeFilter = jobConfig.timeFilter[$routeParams.jobTimeFilter];
        }

        $scope.status = [];
        for(var i in JobList.jobFilter.statusIds){
            for(var j in jobConfig.allStatus){
                if(JobList.jobFilter.statusIds[i] == jobConfig.allStatus[j].value){
                    $scope.status.push(jobConfig.allStatus[j]);
                    break;
                }
            }
        }
        $scope.toggleSelection = function toggleSelection(current) {
            var idx = $scope.status.indexOf(current);
            if (idx > -1) {
              $scope.status.splice(idx, 1);
            }else {
              $scope.status.push(current);
            }
        };



        $scope.tabs=[
          {
            "title":"Jobs",
            "active":true
          },
          {
            "title": "Slow Queries",
            "active": false
          }
        ]

        // projectName from page ctrl
        $scope.state = {loading: false, refreshing: false, filterAttr: 'last_modified', filterReverse: true, reverseColumn: 'last_modified', projectName:$scope.projectModel.selectedProject};

        $scope.list = function (offset, limit) {
            var defer = $q.defer();
            if(!$scope.projectModel.projects.length){
                defer.resolve([]);
              return  defer.promise;
            }
            offset = (!!offset) ? offset : 0;

            var statusIds = [];
            angular.forEach($scope.status, function (statusObj, index) {
                statusIds.push(statusObj.value);
            });

            $scope.cubeName=$scope.cubeName == ""?null:$scope.cubeName;
            JobList.jobFilter.cubeName = $scope.cubeName;
            JobList.jobFilter.timeFilterId = $scope.timeFilter.value;
            JobList.jobFilter.searchModeId = _.indexOf(jobConfig.searchMode, $scope.searchMode);
            JobList.jobFilter.statusIds = statusIds;

            var jobRequest = {
                cubeName: $scope.cubeName,
                projectName: $scope.state.projectName,
                status: statusIds,
                offset: offset,
                limit: limit,
                timeFilter: $scope.timeFilter.value,
                jobSearchMode: $scope.searchMode.value
            };
            $scope.state.loading = true;

            return JobList.list(jobRequest).then(function(resp){
                $scope.state.loading = false;
                if (angular.isDefined($scope.state.selectedJob)) {
                    $scope.state.selectedJob = JobList.jobs[ $scope.state.selectedJob.uuid];
                }
                defer.resolve(resp);
                return defer.promise;
            },function(resp){
              $scope.state.loading = false;
              defer.resolve([]);
              SweetAlert.swal('Oops...', resp, 'error');
              return defer.promise;
            });
        };

        $scope.overview = function () {
          var defer = $q.defer();
          var statusIds = [];
          angular.forEach($scope.status, function (statusObj, index) {
            statusIds.push(statusObj.value);
          });
          var jobRequest = {
            cubeName: $scope.cubeName,
            projectName: $scope.state.projectName,
            status: statusIds,
            timeFilter: $scope.timeFilter.value,
            jobSearchMode: $scope.searchMode.value
          };
          return JobList.overview(jobRequest).then(function(resp){
            defer.resolve(resp);
            return defer.promise;
          },function(resp){
            defer.resolve([]);
            SweetAlert.swal('Oops...', resp, 'error');
            return defer.promise;
          });
        };

        $scope.reload = function () {
            // trigger reload action in pagination directive
            $scope.action.reload = !$scope.action.reload;
            $scope.overview();
        };
        $scope.overview();

        $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
            if(newValue!=oldValue||newValue==null){
                JobList.removeAll();
                $scope.state.projectName = newValue;
                $scope.reload();
            }
        });
        $scope.resume = function (job) {
            SweetAlert.swal({
                title: '',
                text: 'Are you sure to resume the job?',
                type: '',
                showCancelButton: true,
                confirmButtonColor: '#DD6B55',
                confirmButtonText: "Yes",
                closeOnConfirm: true
            }, function(isConfirm) {
              if(isConfirm) {
                loadingRequest.show();
                JobService.resume({jobId: job.uuid}, {}, function (job) {
                  loadingRequest.hide();
                  JobList.jobs[job.uuid] = job;
                  var oldStatus;
                  if (angular.isDefined($scope.state.selectedJob)) {
                    oldStatus = $scope.state.selectedJob.job_status;
                    $scope.state.selectedJob = JobList.jobs[$scope.state.selectedJob.uuid];
                  }
                  angular.forEach(jobConfig.allStatus, function (key) {
                    if (key.name === oldStatus) {
                      JobList.jobsOverview[key.name] -= 1;
                      key.count = "(" + (JobList.jobsOverview[key.name]) + ")";
                    } else if (key.name === job.job_status) {
                      JobList.jobsOverview[key.name] += 1;
                      key.count = "(" + (JobList.jobsOverview[key.name]) + ")";
                    }
                  });
                  MessageBox.successNotify('Job has been resumed successfully!');
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


        $scope.cancel = function (job) {
            SweetAlert.swal({
                title: '',
                text: 'Are you sure to discard the job?',
                type: '',
                showCancelButton: true,
                confirmButtonColor: '#DD6B55',
                confirmButtonText: "Yes",
                closeOnConfirm: true
            }, function(isConfirm) {
              if(isConfirm) {
                loadingRequest.show();
                JobService.cancel({jobId: job.uuid}, {}, function (job) {
                    loadingRequest.hide();
                    $scope.safeApply(function() {
                      JobList.jobs[job.uuid] = job;
                      var oldStatus;
                      if (angular.isDefined($scope.state.selectedJob)) {
                        oldStatus = $scope.state.selectedJob.job_status;
                        $scope.state.selectedJob = JobList.jobs[$scope.state.selectedJob.uuid];
                      }
                      angular.forEach(jobConfig.allStatus, function (key) {
                        if (key.name === oldStatus) {
                          JobList.jobsOverview[key.name] -= 1;
                          key.count = "(" + (JobList.jobsOverview[key.name]) + ")";
                        } else if (key.name === job.job_status) {
                          JobList.jobsOverview[key.name] += 1;
                          key.count = "(" + (JobList.jobsOverview[key.name]) + ")";
                        }
                      });
                    });
                    MessageBox.successNotify('Job has been discarded successfully!');
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

      $scope.pause = function (job) {
        SweetAlert.swal({
          title: '',
          text: 'Are you sure to pause the job?',
          type: '',
          showCancelButton: true,
          confirmButtonColor: '#DD6B55',
          confirmButtonText: "Yes",
          closeOnConfirm: true
        }, function(isConfirm) {
          if(isConfirm) {
            loadingRequest.show();
            JobService.pause({jobId: job.uuid}, {}, function (job) {
              loadingRequest.hide();
              $scope.safeApply(function() {
                JobList.jobs[job.uuid] = job;
                var oldStatus;
                if (angular.isDefined($scope.state.selectedJob)) {
                  oldStatus = $scope.state.selectedJob.job_status;
                  $scope.state.selectedJob = JobList.jobs[$scope.state.selectedJob.uuid];
                }
                angular.forEach(jobConfig.allStatus, function (key) {
                  if (key.name === oldStatus) {
                    JobList.jobsOverview[key.name] -= 1;
                    key.count = "(" + (JobList.jobsOverview[key.name]) + ")";
                  } else if (key.name === job.job_status) {
                    JobList.jobsOverview[key.name] += 1;
                    key.count = "(" + (JobList.jobsOverview[key.name]) + ")";
                  }
                });
              });
              MessageBox.successNotify('Job has been paused successfully!');
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

     $scope.drop = function (job) {
        SweetAlert.swal({
          title: '',
          text: 'Are you sure to drop the job?',
          type: '',
          showCancelButton: true,
          confirmButtonColor: '#DD6B55',
          confirmButtonText: "Yes",
          closeOnConfirm: true
        }, function(isConfirm) {
          if(isConfirm) {
            loadingRequest.show();
            JobService.drop({jobId: job.uuid}, {}, function (job) {
              loadingRequest.hide();
              MessageBox.successNotify('Job has been dropped successfully!');
              $scope.jobList.jobs[job.uuid].dropped = true;
              var oldState = job.job_status;
              angular.forEach(jobConfig.allStatus, function (key) {
                if (key.name === oldState) {
                  JobList.jobsOverview[key.name] -= 1;
                  key.count = "(" + (JobList.jobsOverview[key.name]) + ")";
                }
              });
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

      $scope.resubmit = function (job) {
        SweetAlert.swal({
          title: '',
          text: 'Are you sure to re-submit the job?',
          type: '',
          showCancelButton: true,
          confirmButtonColor: '#DD6B55',
          confirmButtonText: "Yes",
          closeOnConfirm: true
        }, function(isConfirm) {
          if(isConfirm) {
            loadingRequest.show();
            JobService.resubmit({jobId: job.uuid}, {}, function (result) {
              loadingRequest.hide();
              MessageBox.successNotify('Job has been re-submitted successfully!');
            },function(e){
              loadingRequest.hide();
              if(e.data&& e.data.exception){
                var message =e.data.exception;
                var msg = !!(message) ? message : 'Failed to re-submit the job.';
                SweetAlert.swal('Oops...', msg, 'error');
              }else{
                SweetAlert.swal('Oops...', "Failed to re-submit the job.", 'error');
              }
            });
          }
        });
      }

      $scope.diagnosisJob =function(job) {
        if (!job){
          SweetAlert.swal('', "No job selected.", 'info');
          return;
        }
        var downloadUrl = Config.service.url + 'diag/job/'+job.uuid+'/download';
        $window.open(downloadUrl);
      }

        $scope.openModal = function () {
            if (angular.isDefined($scope.state.selectedStep)) {
                if ($scope.state.stepAttrToShow == "output") {
                    $scope.state.selectedStep.loadingOp = true;
                    internalOpenModal();
                    var stepId = $scope.state.selectedStep.sequence_id;
                    JobService.stepOutput({jobId: $scope.state.selectedJob.uuid, propValue: $scope.state.selectedStep.id}, function (result) {
                        if (angular.isDefined(JobList.jobs[result['jobId']])) {
                            var tjob = JobList.jobs[result['jobId']];
                            tjob.steps[stepId].cmd_output = result['cmd_output'];
                            tjob.steps[stepId].loadingOp = false;
                        }
                    },function(e){
                      SweetAlert.swal('Oops...',"Failed to load job info, please check system log for details.", 'error');
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
                    },
                    job: function () {
                        return $scope.state.selectedJob;
                    }
                }
            });
        }
    }
);

var jobStepDetail = function ($scope, $modalInstance, $window, step, attr, job) {
    $scope.step = step;
    $scope.stepAttrToShow = attr;
    $scope.job = job;
    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    }
    $scope.downloadAllLogs =function () {
        var downloadUrl = Config.service.url + 'jobs/'+ job.uuid +'/steps/'+ step.id +'/log' + '?project=' + job.projectName;
        $window.open(downloadUrl);
    }
};
