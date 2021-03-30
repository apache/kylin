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

KylinApp.controller('PageCtrl', function ($scope, $q, AccessService, $modal, $location, $rootScope, $routeParams, $http, UserService, ProjectService, SweetAlert, $cookieStore, $log, kylinConfig, ProjectModel, TableModel, JobList) {

  //init kylinConfig to get kylin.Propeties
  kylinConfig.init().$promise.then(function (data) {
    $log.debug(data);
    kylinConfig.initWebConfigInfo();
    $rootScope.isShowCubeplanner = kylinConfig.getProperty('kylin.cube.cubeplanner.enabled') === 'true';
    $rootScope.isShowDashboard = kylinConfig.getProperty('kylin.web.dashboard-enabled') === 'true';
    JobList.jobFilter.timeFilterId = kylinConfig.getJobTimeFilterId();
  });
  $rootScope.userAction = {
    'islogout': false
  }

  $scope.showHelpInfo = function (helpName) {
    if (helpName === 'aboutKylin') {
      $scope.showAboutKylinDialog()
    }
  }
  var aboutKylinCtr = function ($scope, AdminService, $modalInstance) {
    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    }
    AdminService.getVersionInfo({}, function(versionInfo){
      $scope.versionInfo = versionInfo || {}
    })
  }
  $scope.showAboutKylinDialog = function () {
    $modal.open({
      templateUrl: 'aboutKylin.html',
      controller: aboutKylinCtr,
      windowClass: 'modal-about-kylin',
      resolve: {
      }
    });
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
    ProjectModel.clear();
    JobList.clearJobFilter();
    $rootScope.userAction.islogout = true;
    var logoutURL = Config.service.base;
    if(kylinConfig.getProperty('kylin.security.profile') === 'saml') {
      logoutURL += 'saml/logout';
    } else {
      logoutURL += 'j_spring_security_logout';
    }
    $scope.$emit('event:logoutRequest');
    $http.get(logoutURL).success(function () {
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
  $scope.hasPermission = function (accessType, entity) {
    var curUser = UserService.getCurUser();
    if (!curUser.userDetails) {
      return curUser;
    }
    var hasPermission = false;
    var masks = [];
    for (var i = 2; i < arguments.length; i++) {
      if (arguments[i]) {
        masks.push(arguments[i]);
      }
    }
    var project = ''
    var projectAccesses = ProjectModel.projects || []
    if (accessType === 'cube') {
      project = entity.project
    } else if (accessType === 'project') {
      project = entity && entity.name || entity.selectedProject
    } else if (accessType === 'model') {
      project =  ProjectModel.getProjectByCubeModel(entity.name)
    }
    for(var i = 0;i<projectAccesses.length;i++){
      if(projectAccesses[i].name === project) {
        entity = projectAccesses[i]
        break;
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
    if(!data){
      return '0 KB';
    }
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
      if (!$rootScope.userAction.islogout) {
        //$log.log("project updated in page controller,from:"+oldValue+" To:"+newValue);
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

var projCtrl = function ($scope, $location, $modalInstance, ProjectService, MessageService, projects, project, SweetAlert, ProjectModel, $cookieStore, $route, $timeout, MessageBox) {
  $scope.state = {
    isEdit: false,
    oldProjName: null,
    projectIdx: -1
  };
  $scope.isEdit = false;
  $scope.proj = {name: '', description: '', override_kylin_properties: {}};
  $scope.convertedProperties = [];
  $scope.originOverrideKylinProperties = {};

  if (project) {
    $scope.state.isEdit = true;
    $scope.state.oldProjName = project.name;
    $scope.proj = project;
    angular.copy(project.override_kylin_properties, $scope.originOverrideKylinProperties);

    for (var key in $scope.proj.override_kylin_properties) {
      $scope.convertedProperties.push({
        name: key,
        value: $scope.proj.override_kylin_properties[key]
      });
    }

    for (var i = 0; i < projects.length; i++) {
      if (projects[i].name === $scope.state.oldProjName) {
        $scope.state.projectIdx = i;
        break;
      }
    }
  }

  $scope.createOrUpdate = function () {
	delete $scope.proj.override_kylin_properties[""];
    if ($scope.state.isEdit) {
      ProjectService.update({}, {formerProjectName: $scope.state.oldProjName, projectDescData: angular.toJson($scope.proj)}, function (newProj) {
        MessageBox.successNotify('Project update successfully!');

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
      ProjectService.save({}, {projectDescData: angular.toJson($scope.proj)}, function (newProj) {
        $modalInstance.dismiss('cancel');
        $cookieStore.put("project", newProj.name);
        MessageBox.successAlert("New project created successfully!", function(){
          location.reload();
        });

        $timeout(function () {
          location.reload();
        }, 3000);
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
      project.override_kylin_properties = $scope.originOverrideKylinProperties;
    }
    delete $scope.proj.override_kylin_properties[""];
    $modalInstance.dismiss('cancel');
  };

  $scope.addNewProperty = function () {
    if ($scope.proj.override_kylin_properties.hasOwnProperty('')) {
      return;
    }
    $scope.proj.override_kylin_properties[''] = '';
    $scope.convertedProperties.push({
      name: '',
      value: ''
    });
  };

  $scope.refreshPropertiesObj = function () {
    $scope.proj.override_kylin_properties = {};
    angular.forEach($scope.convertedProperties, function (item, index) {
      $scope.proj.override_kylin_properties[item.name] = item.value;
    })
  };


  $scope.refreshProperty = function (list, index, item) {
    $scope.convertedProperties[index] = item;
    $scope.refreshPropertiesObj();
  };


  $scope.removeProperty = function (arr, index, item) {
    if (index > -1) {
      arr.splice(index, 1);
    }
    delete $scope.proj.override_kylin_properties[item.name];
  }

};
