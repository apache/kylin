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

KylinApp.controller('PageCtrl', function ($scope, $q, AccessService, $modal, $location, $rootScope, $routeParams, $http, UserService, ProjectService, SweetAlert, $cookieStore, $log, kylinConfig, ProjectModel, TableModel) {

  //init kylinConfig to get kylin.Propeties
  kylinConfig.init().$promise.then(function (data) {
    $log.debug(data);
    kylinConfig.initWebConfigInfo();
  });
  $rootScope.userAction={
    'islogout':false
  }

  $scope.kylinConfig = kylinConfig;

  $scope.header = {show: true};

  $scope.$on('$routeChangeSuccess', function ($event, current) {
    $scope.activeTab = current.tab;
    $scope.header.show = ($location.url() && $location.url().indexOf('/home') == -1);
  });

  $scope.config = Config;
  $scope.routeParams = $routeParams;
  $scope.angular = angular;
  $scope.userService = UserService;
  $scope.activeTab = "";
  $scope.projectModel = ProjectModel;
  $scope.tableModel = TableModel;

  // Set up common methods
  $scope.logout = function () {
    //destroy projects info for user
    ProjectModel.clear();

    $rootScope.userAction.islogout = true;
    $scope.$emit('event:logoutRequest');
    $http.get(Config.service.base + 'j_spring_security_logout').success(function () {
      UserService.setCurUser({});
      $scope.username = $scope.password = null;
      $location.path('/login');

      console.debug("Logout Completed.");
    }).error(function () {
      UserService.setCurUser({});
      $scope.username = $scope.password = null;
      $location.path('/login');

      console.debug("Logout Completed.");
    });
    ;
  };

  Messenger.options = {
    extraClasses: 'messenger-fixed messenger-on-bottom messenger-on-right',
    theme: 'air'
  };

  $scope.getInt = function (ivalue) {
    return parseInt(ivalue);
  };

  $scope.getLength = function (obj) {
    if (!obj) {
      return 0;
    }
    var size = 0, key;
    for (key in obj) {
      if (obj.hasOwnProperty(key)) size++;
    }
    return size;
  };

  // common acl methods
  $scope.hasPermission = function (entity) {
    var curUser = UserService.getCurUser();
    if (!curUser.userDetails) {
      return curUser;
    }

    var hasPermission = false;
    var masks = [];
    for (var i = 1; i < arguments.length; i++) {
      if (arguments[i]) {
        masks.push(arguments[i]);
      }
    }

    if (entity) {
      angular.forEach(entity.accessEntities, function (acessEntity, index) {
        if (masks.indexOf(acessEntity.permission.mask) != -1) {
          if ((curUser.userDetails.username == acessEntity.sid.principal) || UserService.hasRole(acessEntity.sid.grantedAuthority)) {
            hasPermission = true;
          }
        }
      });
    }

    return hasPermission;
  };

  $scope.listAccess = function (entity, type) {
    var defer = $q.defer();

    entity.accessLoading = true;
    AccessService.list({type: type, uuid: entity.uuid}, function (accessEntities) {
      entity.accessLoading = false;
      entity.accessEntities = accessEntities;
      defer.resolve();
    });

    return defer.promise;
  };

  // Compute data size so as to auto convert to KB/MB/GB/TB)
  $scope.dataSize = function (data) {
    var size;
    if (data / 1024 / 1024 / 1024 / 1024 >= 1) {
      size = (data / 1024 / 1024 / 1024 / 1024).toFixed(2) + ' TB';
    } else if (data / 1024 / 1024 / 1024 >= 1) {
      size = (data / 1024 / 1024 / 1024).toFixed(2) + ' GB';
    } else if (data / 1024 / 1024 >= 1) {
      size = (data / 1024 / 1024).toFixed(2) + ' MB';
    } else {
      size = (data / 1024).toFixed(2) + ' KB';
    }
    return size;
  };


  $scope.toCreateProj = function () {
    $modal.open({
      templateUrl: 'project.html',
      controller: projCtrl,
      resolve: {
        projects: function () {
          return null;
        },
        project: function () {
          return null;
        }
      }
    });
  };


  $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
    if (newValue != oldValue) {

      //do not update cookie info when logout destroy project info
      if(!$rootScope.userAction.islogout){
        $cookieStore.put("project", $scope.projectModel.selectedProject);
      }
    }

  });

  /*
   *global method for all scope to use
   * */

  //update scope value to view
  $scope.safeApply = function (fn) {
    var phase = this.$root.$$phase;
    if (phase == '$apply' || phase == '$digest') {
      if (fn && (typeof(fn) === 'function')) {
        fn();
      }
    } else {
      this.$apply(fn);
    }
  };

});

var projCtrl = function ($scope, $location, $modalInstance, ProjectService, MessageService, projects, project, SweetAlert, ProjectModel, $cookieStore, $route) {
  $scope.state = {
    isEdit: false,
    oldProjName: null,
    projectIdx: -1
  };
  $scope.isEdit = false;
  $scope.proj = {name: '', description: ''};

  if (project) {
    $scope.state.isEdit = true;
    $scope.state.oldProjName = project.name;
    $scope.proj = project;
    for (var i = 0; i < projects.length; i++) {
      if (projects[i].name === $scope.state.oldProjName) {
        $scope.state.projectIdx = i;
        break;
      }
    }
  }

  $scope.createOrUpdate = function () {
    if ($scope.state.isEdit) {

      var requestBody = {
        formerProjectName: $scope.state.oldProjName,
        newProjectName: $scope.proj.name,
        newDescription: $scope.proj.description
      };
      ProjectService.update({}, requestBody, function (newProj) {
        SweetAlert.swal('Success!', 'Project update successfully!', 'success');

        //update project in project model
        ProjectModel.updateProject($scope.proj.name, $scope.state.oldProjName);
        $cookieStore.put("project", $scope.proj.name);
        ProjectModel.setSelectedProject($scope.proj.name);
        $modalInstance.dismiss('cancel');
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
    else {
      ProjectService.save({}, $scope.proj, function (newProj) {
        SweetAlert.swal('Success!', 'New project created successfully!', 'success');
        $modalInstance.dismiss('cancel');
//                if(projects) {
//                    projects.push(newProj);
//                }
//                ProjectModel.addProject(newProj);
        $cookieStore.put("project", newProj.name);
        location.reload();
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
  };

  $scope.cancel = function () {
    if ($scope.state.isEdit) {
      projects[$scope.state.projectIdx].name = $scope.state.oldProjName;
    }
    $modalInstance.dismiss('cancel');
  };

};
