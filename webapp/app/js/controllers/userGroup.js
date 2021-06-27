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
  .controller('UserGroupCtrl', function ($scope, kylinConfig, UserGroupService, ProjectModel, SweetAlert, $modal, UserService, ResponseUtil) {
    $scope.grouploading = false;
    $scope.userloading = false;
    $scope.dialogActionLoading = false;
    $scope.groups = [];
    $scope.groupsTotal = 0;
    $scope.users = [];
    $scope.usersTotal = 0;
    kylinConfig.init().$promise.then(function() {
      $scope.securityType = kylinConfig.getSecurityType();
      $scope.allowUseUserAndGroupModule = ['testing', 'custom'].indexOf($scope.securityType) >= 0;
    })
    $scope.page = {
      curpage: kylinConfig.page.offset,
      limit:  kylinConfig.page.limit
    }
    $scope.editPage = {
      curpage: kylinConfig.page.offset,
      limit: kylinConfig.page.limit
    }
    $scope.selectGroups = {};
    $scope.selectUsers = {};
    
    
    $scope.filter = {
      name: '',
      groupName: ''
    };
    var createChangePwdMeta = function () {
      return {
        repeatPassword: '',
        newPassword:''
      }
    }
    var createUserMeta = function () {
      return {
        name: '',
        password: '',
        isAdmin: false,
        authorities: ["ALL_USERS"]
      }
    }
    var createGroupMeta = function () {
      return {
        name: ''
      }
    }
    var createFilter = function (offset, limit, pageObj) {
      offset = (!!offset) ? offset : pageObj.curpage;
      if (pageObj) {
        pageObj.curpage = offset;
      }
      offset = offset - 1;
      limit = (!!limit) ? limit : pageObj.limit;
      return {
        offset: offset * limit,
        limit: (offset + 1) * limit,
        name: $scope.filter.name,
        groupName: $scope.tabData.groupName,
        isFuzzMatch: true,
        project: ProjectModel.selectedProject
      };
    }
    $scope.showUserListByGroup = function (groupName) {
      $scope.$emit('change.active', {
        activeTab: 'tab_users',
        groupName: groupName
      });
    };
    $scope.removeGroupFilter = function () {
      $scope.$emit('change.active', {
        activeTab: 'tab_users',
        groupName: ''
      });
      $scope.listUsers();
    }
    $scope.user = createUserMeta();
    $scope.group = createGroupMeta();
    $scope.getGroupList = function (offset, limit, pageObj) {
      var queryParam = createFilter(offset, limit, pageObj);
      $scope.grouploading = true;
      UserGroupService.listGroups(queryParam, function (res) {
        $scope.groups = res && res.data.groups || [];
        $scope.groupsTotal = res.data.size || 0;
        $scope.grouploading = false;
      }, function (res) {
        $scope.grouploading = false;
        ResponseUtil.handleError(res);
      });
    }
    $scope.getUserList = function (offset, limit, pageObj) {
      var queryParam = createFilter(offset, limit, pageObj);
      $scope.grouploading = true;
      UserService.listUsers(queryParam, function (res) {
        $scope.users = res && res.data.users || [];
        $scope.usersTotal = res.data.size || 0;
        $scope.userloading = false;
      }, function (res) {
        $scope.userloading = false;
        ResponseUtil.handleError(res);
      });
    }
    $scope.listUsers = function (offset, limit) {
      $scope.page.curpage = kylinConfig.page.offset
      $scope.getUserList(offset, limit, $scope.page);
    }
    $scope.listGroups = function (offset, limit) {
      $scope.page.curpage = kylinConfig.page.offset
      $scope.getGroupList(offset, limit, $scope.page);
    }
    $scope.listEditUsers = function (offset, limit) {
      $scope.editPage.curpage = kylinConfig.page.offset
      $scope.getUserList(offset, limit, $scope.editPage);
    }
    $scope.listEditGroups = function (offset, limit) {
      $scope.editPage.curpage = kylinConfig.page.offset
      $scope.getGroupList(offset, limit, $scope.editPage);
    }

    $scope.delUser = function (userName) {
      SweetAlert.swal({
        title: '',
        text: "Are you sure to delete the user " + userName + "?",
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "Yes",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){
          UserService.delUser({
            userName: userName
          }, function () {
            SweetAlert.swal('Delete successfuly', null, 'success');
            $scope.listUsers($scope.page.curpage);
          }, function (e) {
            ResponseUtil.handleError(e);
          })
        }
      })
    }
    var updateUser = function (user, isDisable) {
      let updateUser = angular.extend({}, user)
      updateUser.disabled = isDisable
      UserService.updateUser({userName: updateUser.username}, updateUser, function () {
        $scope.listUsers($scope.page.curpage);
        SweetAlert.swal('User status update successfuly', null, 'success');
      }, function (e) {
        ResponseUtil.handleError(e);
      })
    }
    $scope.disableUser = function (user) {
      updateUser(user, true);
    }
    $scope.enableUser = function (user) {
      updateUser(user, false);
    }
    $scope.delGroup = function (groupName) {
      SweetAlert.swal({
        title: '',
        text: "Are you sure to delete the goup " + groupName + "?",
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "Yes",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){
          UserGroupService.delGroup({
            group: groupName
          }, function () {
            SweetAlert.swal('Delete successfuly', null, 'success');
            $scope.listGroups($scope.page.curpage);
          }, function (res) {
            ResponseUtil.handleError(res);
          })
        }
      })
    }
    $scope.isAllchecked = function(selectedItems, items) {
      if (items && items.hasOwnProperty('length')) {
        for(let i = 0; i < items.length; i++) {
          var item = typeof items[i] === "object" ? items[i].username : items[i];
          if (!selectedItems[item]) {
            return false;
          }
        }
      } else {
        for (let i in items) {
          if (!selectedItems[i]) {
            return false;
          }
        }
      }
      return true;
    }
    $scope.selectAllUsers = function(element){
      setTimeout(function() {
        $scope.isChecked = element.checked;
        $scope.users.forEach((u) => {
          $scope.selectUsers[u.username] = $scope.isChecked;
        })
        $scope.$apply()
      }, 1);
    }
    $scope.selectAllGroups = function(element){
      setTimeout(function() {
        $scope.isChecked = element.checked;
        for (let g in $scope.groups) {
          $scope.selectGroups[g] = $scope.isChecked;
        }
        $scope.$apply()
      }, 1);
    }
    $scope.selectGroup = function (groupName) {
      $scope.selectGroups[groupName] = !$scope.selectGroups[groupName];
    }
    var userEditCtr = function ($scope, $modalInstance, UserService,SweetAlert, kylinConfig) {
      $scope.userPattern = /^[\w.@]+$/;
      $scope.groupPattern = /^[\w.@]+$/;
      $scope.pwdPattern = /^(?=.*\d)(?=.*[a-z])(?=.*[~!@#$%^&*(){}|:"<>?[\];',./`]).{8,}$/;
      $scope.saveAssignGroup = function () {
        $scope.user.authorities = [];
        for (let i in $scope.selectGroups) {
          if ($scope.selectGroups[i]) {
            $scope.user.authorities.push(i);
          }
        }
        $scope.dialogActionLoading = true;
        UserService.assignGroup({
          userName: $scope.user.name
        }, $scope.user, function () {
          $scope.dialogActionLoading = false;
          $modalInstance.dismiss('cancel');
          SweetAlert.swal('Assign successfuly', null, 'success');
          $scope.listUsers($scope.page.curpage);
        }, function (res){
          $scope.dialogActionLoading = false;
          ResponseUtil.handleError(res);
        })
      }

      $scope.saveAssignUser = function () {
        let users = []
        for (let i in $scope.selectUsers) {
          if ($scope.selectUsers[i]) {
            users.push(i);
          }
        }
        $scope.dialogActionLoading = true;
        UserGroupService.assignUsers({
          group: $scope.group.name
        }, users, function () {
          $scope.dialogActionLoading = false;
          $modalInstance.dismiss('cancel');
          SweetAlert.swal('Assign successfuly', null, 'success');
          $scope.listGroups($scope.page.curpage);
        }, function (res){
          $scope.dialogActionLoading = false;
          ResponseUtil.handleError(res);
        })
      }

      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      }
      $scope.saveUser = function () {
        $scope.dialogActionLoading = true;
        if ($scope.user.isAdmin) {
          $scope.user.authorities.push('ROLE_ADMIN');
        }
        UserService.addUser({
          userName: $scope.user.name
        }, $scope.user, function() {
          $scope.dialogActionLoading = false;
          $modalInstance.dismiss('cancel');
          SweetAlert.swal('Add user successfuly', null, 'success');
          $scope.listUsers();
        }, function (res){
          $scope.dialogActionLoading = false;
          ResponseUtil.handleError(res)
        })
      }
      $scope.saveGroup = function () {
        $scope.dialogActionLoading = true;
        UserGroupService.addGroup({
          group: $scope.group.name
        }, null, function () {
          $modalInstance.dismiss('cancel');
          SweetAlert.swal('Add group successfuly', null, 'success');
          $scope.listGroups();
          $scope.dialogActionLoading = false;
        }, function (res){
          $scope.dialogActionLoading = false;
          ResponseUtil.handleError(res)
        })
      }
      $scope.saveNewPassword = function () {
        $scope.dialogActionLoading = true;
        UserService.changePwd($scope.changePwdUser, function () {
          $modalInstance.dismiss('cancel');
          SweetAlert.swal('Change password successfuly', null, 'success');
          $scope.listUsers();
          $scope.dialogActionLoading = false;
        }, function (e) {
          $scope.dialogActionLoading = false;
          ResponseUtil.handleError(e);
        })
      }
    }
    $scope.createUser = function () {
      $scope.user = createUserMeta();
      $modal.open({
        templateUrl: 'addUser.html',
        controller: userEditCtr,
        scope: $scope
      });
    }
    $scope.createGroup = function () {
      $scope.group = createGroupMeta();
      $modal.open({
        templateUrl: 'addGroup.html',
        controller: userEditCtr,
        scope: $scope
      });
    }
    $scope.assignToGroup = function (userName, authorities) {
      $scope.listEditGroups();
      $scope.user.name = userName;
      $scope.selectGroups = {};
      authorities.forEach(auth => {
        $scope.selectGroups[auth.authority] = true;
      })
      $modal.open({
        templateUrl: 'assignGroup.html',
        controller: userEditCtr,
        scope: $scope
      });
    }
    $scope.assignToUser = function (groupName) {
      $scope.listEditUsers();
      $scope.group.name = groupName;
      UserGroupService.getUsersByGroup({
        group: groupName
      }, {}, function(res) {
        ResponseUtil.handleSuccess(res, function(data) {
          let groupUsers = data.users || []
          $scope.selectUsers = {};
          groupUsers.forEach(function(user){
            $scope.selectUsers[user.username] = true;
          })
          $modal.open({
            templateUrl: 'assignUser.html',
            controller: userEditCtr,
            scope: $scope
          });
        })
      })  
    }
    $scope.changePwdUser = createChangePwdMeta()
    $scope.changePwd = function (user) {
      $scope.changePwdUser.username = user.username;
      $scope.changePwdUser.password = user.password;
      $scope.changePwdUser.repeatPassword = '';
      $scope.changePwdUser.newPassword = '';
      $modal.open({
        templateUrl: 'changePwd.html',
        controller: userEditCtr,
        scope: $scope
      });
    }
  });
