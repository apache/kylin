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

/*
 *jobListModel will manage data in list job page
 */

KylinApp.service('JobList',function(JobService, $q, kylinConfig, jobConfig){
    var _this = this;
    this.jobs={};
    this.jobsOverview={};
    this.jobFilter = {
        cubeName : null,
        timeFilterId : kylinConfig.getJobTimeFilterId(),
        searchModeId: 4,
        statusIds: []
    };

    this.clearJobFilter = function(){
        this.jobFilter = {
          cubeName : null,
          timeFilterId : kylinConfig.getJobTimeFilterId(),
          searchModeId: 4,
          statusIds: []
        };
    };

    this.list = function(jobRequest){
        var defer = $q.defer();
        console.log();
        JobService.list(jobRequest, function (jobs) {
            angular.forEach(jobs, function (job) {
                var id = job.uuid;
                if (angular.isDefined(_this.jobs[id])) {
                    if (job.last_modified != _this.jobs[id].last_modified) {
                        _this.jobs[id] = job;
                    } else {
                    }
                } else {
                    _this.jobs[id] = job;
                }
                _this.jobs[id].dropped = false;
            });
            defer.resolve(jobs.length);
        },function(e){
          var msg = 'failed to load jobs';
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            msg = !!(message) ? message : msg;
          }
          defer.reject(msg);
        });
        return defer.promise;
    };

    this.overview = function(jobRequest){
      var defer = $q.defer();
      JobService.overview(jobRequest, function (jobsOverview) {
        angular.forEach(jobConfig.allStatus, function (key) {
          if (angular.isDefined(jobsOverview[key.name])) {
            key.count = "(" + jobsOverview[key.name] + ")";
            _this.jobsOverview[key.name] = jobsOverview[key.name];
          } else {
            key.count = "";
          }
        });
        defer.resolve(jobsOverview);
      },function(e){
        var msg = 'failed to load job overview';
        if (e.data && e.data.exception) {
          var message = e.data.exception;
          msg = !!(message) ? message : msg;
        }
        defer.reject(msg);
      });
      return defer.promise;
    };

    this.removeAll = function(){
        _this.jobs={};
        _this.jobsOverview={};
    };
});
