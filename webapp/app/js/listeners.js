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

KylinApp.run(function ($rootScope, $http, $location, UserService, AuthenticationService, MessageService, $cookieStore, ProjectService, ProjectModel, AccessService, SweetAlert, loadingRequest) {

  $rootScope.permissions = {
    READ: {name: 'CUBE QUERY', value: 'READ', mask: 1},
    MANAGEMENT: {name: 'CUBE EDIT', value: 'MANAGEMENT', mask: 32},
    OPERATION: {name: 'CUBE OPERATION', value: 'OPERATION', mask: 64},
    ADMINISTRATION: {name: 'CUBE ADMIN', value: 'ADMINISTRATION', mask: 16}
  };

  $rootScope.$on("$routeChangeStart", function () {
    AuthenticationService.ping(function (data) {
      UserService.setCurUser(data);
      if (!data.userDetails) {
        $location.path(UserService.getHomePage());
      } else {
        //get project info when login
        if (!ProjectModel.projects.length && !$rootScope.userAction.islogout) {

          loadingRequest.show();
          ProjectService.listReadable({}, function (projects) {
            loadingRequest.hide();

            if (!projects.length) {
              return;
            }

            var _projects = [];
            _projects = _.sortBy(projects, function (i) {
              return i.name.toLowerCase();
            });
            ProjectModel.setProjects(_projects);
            var projectInCookie = $cookieStore.get("project");
            if (projectInCookie && ProjectModel.getIndex(projectInCookie) == -1) {
              projectInCookie = null;
            }
            var selectedProject = projectInCookie != null ? projectInCookie : null;
            if (projectInCookie != null) {
              selectedProject = projectInCookie;
            } else if (UserService.hasRole('ROLE_ADMIN')) {
              selectedProject = null;
            } else {
              selectedProject = ProjectModel.projects[0].name
            }

            //var selectedProject = ProjectModel.selectedProject != null ? ProjectModel.selectedProject : projectInCookie != null ? projectInCookie : ProjectModel.projects[0].name;
            ProjectModel.setSelectedProject(selectedProject);
            angular.forEach(ProjectModel.projects, function (project, index) {
              project.accessLoading = true;
              AccessService.list({type: 'ProjectInstance', uuid: project.uuid}, function (accessEntities) {
                project.accessLoading = false;
                project.accessEntities = accessEntities;
              });
            });

          }, function (e) {
            loadingRequest.hide();
            $location.path(UserService.getHomePage());
          });
        }
      }
    });


    if ($location.url() == '' || $location.url() == '/') {
      AuthenticationService.ping(function (data) {
        UserService.setCurUser(data);
        $location.path(UserService.getHomePage());
      });
      return;
    }
  });

  /**
   * Holds all the requests which failed due to 401 response.
   */
  $rootScope.requests401 = [];

  $rootScope.$on('event:loginRequired', function () {
    $rootScope.requests401 = [];
    $location.path('/login');
    loadingRequest.hide();
  });

  /**
   * On 'event:loginConfirmed', resend all the 401 requests.
   */
  $rootScope.$on('event:loginConfirmed', function () {
    var i,
      requests = $rootScope.requests401,
      retry = function (req) {
        $http(req.config).then(function (response) {
          req.deferred.resolve(response);
        });
      };

    for (i = 0; i < requests.length; i += 1) {
      retry(requests[i]);
    }
    $rootScope.requests401 = [];
  });

  /**
   * On 'logoutRequest' invoke logout on the server.
   */
  $rootScope.$on('event:logoutRequest', function () {
    httpHeaders.common['Authorization'] = null;
  });

  if ($location.url() == '' || $location.url() == '/') {
    AuthenticationService.ping(function (data) {
      UserService.setCurUser(data);
      $location.path(UserService.getHomePage());
    });
    return;
  }

  /**
   * On 'event:forbidden', resend all the 403 requests.
   */
  $rootScope.$on('event:forbidden', function (event, message) {
    var msg = !!(message) ? message : 'You don\' have right to take the action.';
    SweetAlert.swal('Oops...', 'Permission Denied: ' + msg, 'error');

  });

});
