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

KylinApp.controller('ModelsCtrl', function ($scope, $q, $routeParams, $location, $window,$modal, MessageService, CubeDescService, CubeService, JobService, UserService,  ProjectService,SweetAlert,loadingRequest,$log,modelConfig,ProjectModel,ModelService,MetaModel,modelsManager,cubesManager,TableModel,$animate) {

        //tree data

        $scope.cubeSelected = false;
        $scope.cube = {};

        $scope.showModels=true;

        //tracking data loading status in /models page
        $scope.tableModel = TableModel;

        $scope.toggleTab = function(showModel){
            $scope.showModels = showModel;
        }

        $scope.modelsManager = modelsManager;
        $scope.cubesManager = cubesManager;
        $scope.modelConfig = modelConfig;
        modelsManager.removeAll();
        $scope.loading = false;
        $scope.action = {};
        $scope.window = 0.68 * $window.innerHeight;


        $scope.init = function(){

            var queryParam = {};

            queryParam.projectName = $scope.projectModel.selectedProject;
            modelsManager.list(queryParam).then(function(resp){
              modelsManager.loading = false;
            });

        };

        $scope.init();

        $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
          if (newValue != oldValue || newValue == null) {
            modelsManager.removeAll();
            $scope.init();
          }

        });

    $scope.status = {
        isopen: true
    };

    $scope.toggled = function(open) {
        $log.log('Dropdown is now: ', open);
    };

    $scope.toggleDropdown = function($event) {
        $event.preventDefault();
        $event.stopPropagation();
        $scope.status.isopen = !$scope.status.isopen;
    };

  $scope.hideSideBar = false;
  $scope.toggleModelSideBar = function(){
    $scope.hideSideBar = !$scope.hideSideBar;
  }

  $scope.dropModel = function (model) {

    SweetAlert.swal({
      title: '',
      text: "Are you sure to drop this model?",
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function(isConfirm) {
      if(isConfirm){

        loadingRequest.show();
        ModelService.drop({modelId: model.name}, {}, function (result) {
          loadingRequest.hide();
//                    CubeList.removeCube(cube);
          SweetAlert.swal('Success!', 'Model drop is done successfully', 'success');
          location.reload();
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
  };

  $scope.openModal = function (model) {
    $scope.modelsManager.selectedModel = model;
    $modal.open({
      templateUrl: 'modelDetail.html',
      controller:ModelDetailModalCtrl,
      resolve: {
        scope: function () {
          return $scope;
        }
      }
    });
  };

  var ModelDetailModalCtrl = function ($scope,$location, $modalInstance,scope) {
    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    };
  };

});
