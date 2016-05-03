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
    .controller('BadQueryCtrl', function ($scope,BadQueryService, $q, $routeParams, $interval, $modal, ProjectService, MessageService, JobService,SweetAlert,ProjectModel,$window) {

      $scope.badQueryList = [];

      $scope.bqstate={
        loading:true,
        filterAttr: 'last_modified',
        filterReverse: true,
        reverseColumn: 'last_modified'
      }

      $scope.list = function(){
        var _project = ProjectModel.selectedProject;
          $scope.badQueryList = [];
        if (_project == null){
          $scope.bqstate.loading = false;
          return;
        }
        BadQueryService.list({
          entity:_project
        }, function (queryList) {
          angular.forEach(queryList, function (query) {
            $scope.badQueryList.push(query);
          })
            $scope.bqstate.loading = false;
        }, function (e) {
          $scope.bqstate.loading = false;
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Failed to load query.';
            SweetAlert.swal('Oops...', msg, 'error');
          } else {
            SweetAlert.swal('Oops...', "Failed to load query.", 'error');
          }
        });
      }

      $scope.list();

      $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
        if(newValue!=oldValue||newValue==null){
          $scope.list();
        }

      });

    });
