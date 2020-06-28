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
    .controller('ProjectCtrl', function ($scope, $modal, $q, ProjectService, MessageService,SweetAlert,$log,kylinConfig,projectConfig,ProjectModel, MessageBox) {

        $scope.projects = [];
        $scope.loading = false;
        $scope.projectConfig = projectConfig;

        $scope.state = { filterAttr: 'name', filterReverse: true, reverseColumn: 'name'};

        $scope.list = function (offset, limit) {
            offset = (!!offset) ? offset : 0;
            limit = (!!limit) ? limit : 20;
            var defer = $q.defer();
            var queryParam = {offset: offset, limit: limit};

            $scope.loading = true;
            ProjectService.listReadable(queryParam, function (projects) {
                $scope.projects = $scope.projects.concat(projects);
                angular.forEach(projects, function (project) {
                    $scope.listAccess(project, 'ProjectInstance');
                });
                $scope.loading = false;
                defer.resolve(projects.length);
            });

            return defer.promise;
        }

        $scope.toEdit = function(project) {
            $modal.open({
                templateUrl: 'project.html',
                controller: projCtrl,
                resolve: {
                    projects: function () {
                        return $scope.projects;
                    },
                    project: function(){
                        return project;
                    }
                }
            });
        }

        $scope.delete = function(project) {
            SweetAlert.swal({
                title: '',
                text: 'Are you sure to delete ?',
                type: '',
                showCancelButton: true,
                confirmButtonColor: '#DD6B55',
                confirmButtonText: "Yes",
                closeOnConfirm: true
            }, function(isConfirm) {
                if(isConfirm){
                    ProjectService.delete({projecId: project.name}, function(){
                        var pIndex = $scope.projects.indexOf(project);
                        if (pIndex > -1) {
                            $scope.projects.splice(pIndex, 1);
                        }
                        ProjectModel.removeProject(project.name);
                        MessageBox.successNotify("Project [" + project.name + "] has been deleted successfully!");
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
            });
        }

        $scope.getMapLength = function(map) {
        	  return Object.keys(map).length;
        }

        $scope.getOwnerString = function (project) {
            project.newOwner = project.owner;
        };

        $scope.updateOwner = function (project) {
            ProjectService.updateOwner({projecId: project.name}, project.newOwner, function () {
                project.owner = project.newOwner;
                MessageBox.successNotify('Owner updated successfully!');
            },function(e){
                if(e.data&& e.data.exception){
                    var message =e.data.exception;
                    var msg = !!(message) ? message : 'Failed to take action.';
                    SweetAlert.swal('Oops...', msg, 'error');
                } else{
                    SweetAlert.swal('Oops...', "Failed to take action.", 'error');
                }
            });
        };
    }
);

