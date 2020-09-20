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

KylinApp.controller('AdminCtrl', function ($scope, AdminService, CacheService, TableService, loadingRequest, MessageService, ProjectService, $modal, SweetAlert,kylinConfig,ProjectModel,$window, MessageBox) {
  $scope.configStr = "";
  $scope.envStr = "";
  $scope.active = {
    tab_instance: true
  }
  $scope.tabData = {}
  $scope.activateTab = function(tab) {
    $scope.active = {}; //reset
    $scope.active[tab] = true;
  }
  $scope.$on('change.active', function(event, data) {
    $scope.activateTab(data.activeTab);
    $scope.tabData.groupName = data.groupName
  });
  $scope.isCacheEnabled = function(){
    return kylinConfig.isCacheEnabled();
  }

  $scope.getEnv = function () {
    AdminService.env({}, function (env) {
      $scope.envStr = env.env;
      MessageBox.successNotify('Server environment get successfully', "server-env");
//            SweetAlert.swal('Success!', 'Server environment get successfully', 'success');
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

  $scope.getConfig = function () {
    AdminService.config({}, function (config) {
      $scope.configStr = config.config;
      MessageBox.successNotify('Server config get successfully', "server-config");
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

  $scope.reloadConfig = function () {
    SweetAlert.swal({
      title: '',
      text: 'Are you sure to reload config',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {
        CacheService.reloadConfig({}, function () {
          MessageBox.successNotify('Config reload successfully');
          $scope.getConfig();
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
    });
  }

  $scope.reloadMeta = function () {
    SweetAlert.swal({
      title: '',
      text: 'Are you sure to reload metadata and clean cache?',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {
        CacheService.clean({}, function () {
          MessageBox.successNotify('Cache reload successfully');
          ProjectService.listReadable({}, function(projects) {
            ProjectModel.setProjects(projects);
          });
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

    });
  }

  $scope.calCardinality = function (tableName) {
    var _project = ProjectModel.selectedProject;
      if (_project == null){
        SweetAlert.swal('', "No project selected.", 'info');
          return;
        }
    $modal.open({
      templateUrl: 'calCardinality.html',
      controller: CardinalityGenCtrl,
      resolve: {
        tableName: function () {
          return tableName;
        },
        scope: function () {
          return $scope;
        }
      }
    });
  }

  $scope.cleanStorage = function () {
    SweetAlert.swal({
      title: '',
      text: 'Are you sure to clean up unused HDFS and HBase space?',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {
        AdminService.cleanStorage({}, function () {
          MessageBox.successNotify('Storage cleaned successfully!');
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
    });
  }

  $scope.disableCache = function () {
    SweetAlert.swal({
      title: '',
      text: 'Are you sure to disable query cache?',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {
        AdminService.updateConfig({}, {key: 'kylin.query.cache-enabled', value: false}, function () {
          MessageBox.successNotify('Cache disabled successfully!');
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

    });

  }

  $scope.enableCache = function () {
    SweetAlert.swal({
      title: '',
      text: 'Are you sure to enable query cache?',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {
        AdminService.updateConfig({}, {key: 'kylin.query.cache-enabled', value: true}, function () {
          MessageBox.successNotify('Cache enabled successfully!');
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

    });

  }

  $scope.toSetConfig = function () {
    $modal.open({
      templateUrl: 'updateConfig.html',
      controller: updateConfigCtrl,
      scope: $scope,
      resolve: {}
    });
  }

  var CardinalityGenCtrl = function ($scope, $modalInstance, tableName, MessageService, MessageBox) {
    $scope.tableName = tableName;
    $scope.delimiter = 0;
    $scope.format = 0;
    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    };
    $scope.calculate = function () {
      $modalInstance.dismiss();
      loadingRequest.show();
      var _project = ProjectModel.selectedProject;
      if (_project == null){
        SweetAlert.swal('', "No project selected.", 'info');
        return;
      }
      TableService.genCardinality({tableName: $scope.tableName, pro: _project}, {
        delimiter: $scope.delimiter,
        format: $scope.format
      }, function (result) {
        loadingRequest.hide();
        MessageBox.successNotify('Cardinality job was calculated successfully. . Click Refresh button ...');
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
  };

  var updateConfigCtrl = function ($scope, $modalInstance, AdminService, MessageService, MessageBox) {
    $scope.state = {
      key: null,
      value: null
    };
    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    };
    $scope.update = function () {
      AdminService.updateConfig({}, {key: $scope.state.key, value: $scope.state.value}, function (result) {
        MessageBox.successNotify('Config updated successfully!');
        $modalInstance.dismiss();
        $scope.getConfig();
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

  $scope.downloadBadQueryFiles = function(){
    var _project = ProjectModel.selectedProject;
    if (_project == null){
      SweetAlert.swal('', "No project selected.", 'info');
      return;
    }
    var downloadUrl = Config.service.url + 'diag/project/'+_project+'/download';
    $window.open(downloadUrl);
  }

  $scope.openSparderUrl = function(){
    AdminService.openSparderUrl({}, function (urlString) {
      if (urlString.url != "") {
        $window.open(urlString.url);
      } else {
        SweetAlert.swal('Oops...', 'There is no sparder tracking url.', 'warning');
      }
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

  $scope.isCuratorScheduler = function() {
    return kylinConfig.getProperty("kylin.job.scheduler.default") === "100";
  }

  $scope.getEnv();
  $scope.getConfig();
});
