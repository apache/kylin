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

KylinApp.controller('ModelsCtrl', function ($scope, $q, $routeParams, $location, $window, $modal, MessageService, CubeDescService, CubeService, JobService, UserService, ProjectService, SweetAlert, loadingRequest, $log, modelConfig, ProjectModel, ModelService, MetaModel, modelsManager, cubesManager, TableModel, AccessService, MessageBox, CubeList) {

  //tree data

  $scope.cubeSelected = false;
  $scope.cube = {};

  $scope.showModels = true;

  //tracking data loading status in /models page
  $scope.tableModel = TableModel;

  $scope.toggleTab = function (showModel) {
    $scope.showModels = showModel;
  }

  $scope.modelsManager = modelsManager;
  $scope.cubesManager = cubesManager;
  $scope.modelConfig = modelConfig;
  modelsManager.removeAll();
  $scope.loading = false;
  $scope.window = 0.68 * $window.innerHeight;


  //trigger init with directive []
  $scope.list = function () {
    var defer = $q.defer();
    var queryParam = {};
    if (!$scope.projectModel.isSelectedProjectValid()) {
      defer.resolve([]);
      return defer.promise;
    }

    if (!$scope.projectModel.projects.length) {
      defer.resolve([]);
      return defer.promise;
    }
    queryParam.projectName = $scope.projectModel.selectedProject;
    return modelsManager.list(queryParam).then(function (resp) {
      defer.resolve(resp);
      modelsManager.loading = false;
      return defer.promise;
    });

  };

  $scope.list();

  $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
    if (newValue != oldValue || newValue == null) {
      modelsManager.removeAll();
      $scope.list();
    }

  });

  $scope.status = {
    isopen: true
  };

  $scope.toggled = function (open) {
    $log.log('Dropdown is now: ', open);
  };

  $scope.toggleDropdown = function ($event) {
    $event.preventDefault();
    $event.stopPropagation();
    $scope.status.isopen = !$scope.status.isopen;
  };

  $scope.hideSideBar = false;
  $scope.toggleModelSideBar = function () {
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
    }, function (isConfirm) {
      if (isConfirm) {

        loadingRequest.show();
        ModelService.drop({modelId: model.name}, {}, function (result) {
          loadingRequest.hide();
//                    CubeList.removeCube(cube);
          MessageBox.successNotify('Model drop is done successfully');
          location.reload();
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
  };

  $scope.editModel = function(model, isEditJson){
    var cubename = [];
    var modelstate=false;
    var i=0;

    CubeService.list({modelName:model.name,projectName:$scope.projectModel.selectedProject}, function (_cubes) {
      model.cubes = _cubes;

      if (model.cubes.length != 0) {
        angular.forEach(model.cubes,function(cube){
          if (cube.status=="READY"){
            modelstate=true;
            cubename[i] =cube.name;
            i++;
          }
        })
      }

      if (isEditJson) {
        $location.path("/models/edit/" + model.name + "/descriptionjson");
      } else {
        $location.path("/models/edit/" + model.name);
      }
    })

  };

  $scope.cloneModel = function(model){
    $modal.open({
      templateUrl: 'modelClone.html',
      controller: modelCloneCtrl,
      windowClass:"clone-cube-window",
      resolve: {
        model: function () {
          return model;
        }
      }
    });
  }

  $scope.listCubes = function(model) {
    var defer = $q.defer();
    var queryParam = {modelName: model.name};
    if (!$scope.projectModel.isSelectedProjectValid() || !$scope.projectModel.projects.length) {
      SweetAlert.swal('Oops...', "Please select target project.", 'info');
      defer.resolve([]);
      return defer.promise;
    }

    queryParam.projectName = $scope.projectModel.selectedProject;

    $scope.loading = true;
    CubeList.removeAll();
    return CubeList.list(queryParam).then(function (resp) {
      angular.forEach(CubeList.cubes, function(cube, index) {
      })

      $scope.loading = false;
      defer.resolve(resp);
      return defer.promise;

    }, function(resp) {
      $scope.loading = false;
      defer.resolve([]);
      SweetAlert.swal('Oops...', resp, 'error');
      return defer.promise;
    });
  }


  $scope.openModal = function (model) {
    $scope.modelsManager.selectedModel = model;
    $modal.open({
      templateUrl: 'modelDetail.html',
      controller: ModelDetailModalCtrl,
      resolve: {
        scope: function () {
          return $scope;
        }
      }
    });
  };

  function changePositionOfScrollBar(){
    //get which button be clicked
    var btn = window.event.srcElement || window.event.target;
    //get current position of scroll bar
    var scrollTop =$("#cube_model_trees").scrollTop();
    //get total length of scroll bar
    var scrollHeight  = document.getElementById('cube_model_trees').scrollHeight;
    //get the position of clicked button relative to the top of window
    var offsetTop  =$(btn).offset().top;
    //get the position of the container relative to the top of window
    var treeOffsetTop = $("#cube_model_trees").offset().top;

    //distance from button to the top of tree model container
    var minor = offsetTop - treeOffsetTop;
    //height of tree model container
    var  viewH =$("#cube_model_trees").height();

    //change scroll bar to show the dropdown menu
    if(minor + 100 > viewH){//100 is the height of dropdowm menu
      if((scrollHeight - scrollTop - viewH)>=minor+100-viewH){
        document.getElementById('cube_model_trees').scrollTop+=(minor+120-viewH);
      }else{
        var node=document.createElement("LI");
        node.style.height = (minor+120-viewH)+"px";
        document.getElementById("models-tree").appendChild(node);
        var  viewH =$("#cube_model_trees").height();//可见高度
        document.getElementById('cube_model_trees').scrollTop+=(minor+120-viewH);

      }
    }
  }

  $scope.listModelAccess = function (model) {
    changePositionOfScrollBar();

    if(model.uuid){
      AccessService.list({type: "DataModelDesc", uuid: model.uuid}, function (accessEntities) {
        model.accessEntities = accessEntities;
        try {
          if (!model.owner) {
            model.owner = accessEntities[0].sid.principal;
          }
        } catch (error) {
          $log.error("No acl info.");
        }
      })
    }


  };

  var ModelDetailModalCtrl = function ($scope, $location, $modalInstance, scope) {
    modelsManager.selectedModel.visiblePage='metadata';
    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    };
  };

});


var modelCloneCtrl = function ($scope, $modalInstance, CubeService, MessageService, $location, model, MetaModel, SweetAlert,ProjectModel, loadingRequest,ModelService, MessageBox) {
  $scope.projectModel = ProjectModel;

  $scope.targetObj={
    modelName:model.name+"_clone",
    targetProject:$scope.projectModel.selectedProject
  }

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.cloneModel = function(){

    if(!$scope.targetObj.targetProject){
      SweetAlert.swal('Oops...', "Please select target project.", 'info');
      return;
    }

    $scope.modelRequest = {
      modelName:$scope.targetObj.modelName,
      project:$scope.targetObj.targetProject
    }

    SweetAlert.swal({
      title: '',
      text: 'Are you sure to clone the model? ',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {

        loadingRequest.show();
        ModelService.clone({modelId: model.name}, $scope.modelRequest, function (result) {
          loadingRequest.hide();
          MessageBox.successNotify('Clone model successfully');
          location.reload();
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

}
