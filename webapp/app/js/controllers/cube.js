'use strict';

KylinApp.controller('CubeCtrl', function ($scope, AccessService, MessageService, CubeService, TableService, CubeGraphService, UserService, AuthenticationService,SweetAlert) {
    $scope.newAccess = null;
    $scope.state = {jsonEdit: false};

    $scope.buildGraph = function (cube) {
        CubeGraphService.buildTree(cube);
    };

    $scope.getCubeSql = function (cube) {
        CubeService.getSql({cubeId: cube.name, propValue: "null"}, function (sql) {
            cube.sql = sql.sql;
        },function(e){
            if(e.data&& e.data.exception){
                var message =e.data.exception;
                var msg = !!(message) ? message : 'Failed to take action.';
                SweetAlert.swal('Oops...', msg, 'error');
            }else{
                SweetAlert.swal('Oops...', "Failed to take action.", 'error');
            }
        });
    };

    $scope.getNotifyListString = function (cube) {
        if (cube.detail.notify_list) {
            cube.notifyListString = cube.detail.notify_list.join(",");
        }
        else {
            cube.notifyListString = "";
        }
    };

    $scope.cleanStatus = function(cube){

        if (!cube)
        {
            return;
        }
        var newCube = jQuery.extend(true, {}, cube);
        delete newCube.project;

        angular.forEach(newCube.dimensions, function(dimension, index){
            delete dimension.status;
        });

        return newCube;
    };

    $scope.updateNotifyList = function (cube) {
        cube.detail.notify_list = cube.notifyListString.split(",");
        CubeService.updateNotifyList({cubeId: cube.name}, cube.detail.notify_list, function () {
//            MessageService.sendMsg('Notify List updated successfully!', 'success', {});
            SweetAlert.swal('Success!', 'Notify List updated successfully!', 'success');
        },function(e){
            if(e.data&& e.data.exception){
                var message =e.data.exception;
                var msg = !!(message) ? message : 'Failed to take action.';
                SweetAlert.swal('Oops...', msg, 'error');
            }else{
                SweetAlert.swal('Oops...', "Failed to take action.", 'error');
            }
        });
    };

    $scope.getHbaseInfo = function (cube) {
        if (!cube.hbase) {
            CubeService.getHbaseInfo({cubeId: cube.name, propValue: null, action: null}, function (hbase) {
                cube.hbase = hbase;

                // Calculate cube total size based on each htable.
                var totalSize = 0;
                hbase.forEach(function(t) {
                    totalSize += t.tableSize;
                });
                cube.totalSize = totalSize;
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
    };
});