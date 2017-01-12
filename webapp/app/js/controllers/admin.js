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

KylinApp.controller('AdminCtrl', function ($scope, AdminService, CacheService, TableService, loadingRequest, MessageService, $modal, SweetAlert,kylinConfig,ProjectModel,$window) {
  $scope.configStr = "";
  $scope.envStr = "";

  $scope.isCacheEnabled = function(){
    console.log("cache enabled?:"+kylinConfig.isCacheEnabled());
    return kylinConfig.isCacheEnabled();
  }

  $scope.getEnv = function () {
    AdminService.env({}, function (env) {
      $scope.envStr = env.env;
      MessageService.sendMsg('Server environment get successfully', 'success', {});
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
      MessageService.sendMsg('Server config get successfully', 'success', {});
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
          SweetAlert.swal('Success!', 'config reload successfully', 'success');
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
          SweetAlert.swal('Success!', 'Cache reload successfully', 'success');
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
          SweetAlert.swal('Success!', 'Storage cleaned successfully!', 'success');
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
          SweetAlert.swal('Success!', 'Cache disabled successfully!', 'success');
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
          SweetAlert.swal('Success!', 'Cache enabled successfully!', 'success');
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
      resolve: {}
    });
  }

  var CardinalityGenCtrl = function ($scope, $modalInstance, tableName, MessageService) {
    $scope.tableName = tableName;
    $scope.delimiter = 0;
    $scope.format = 0;
    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    };
    $scope.calculate = function () {
      $modalInstance.dismiss();
      loadingRequest.show();
      TableService.genCardinality({tableName: $scope.tableName}, {
        delimiter: $scope.delimiter,
        format: $scope.format
      }, function (result) {
        loadingRequest.hide();
        SweetAlert.swal('Success!', 'Cardinality job was calculated successfully. . Click Refresh button ...', 'success');
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

  var updateConfigCtrl = function ($scope, $modalInstance, AdminService, MessageService) {
    $scope.state = {
      key: null,
      value: null
    };
    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    };
    $scope.update = function () {


      AdminService.updateConfig({}, {key: $scope.state.key, value: $scope.state.value}, function (result) {
        SweetAlert.swal('Success!', 'Config updated successfully!', 'success');
        $modalInstance.dismiss();
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


  $scope.getEnv();
  $scope.getConfig();
});
