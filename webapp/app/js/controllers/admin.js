'use strict';

KylinApp.controller('AdminCtrl', function ($scope,AdminService, CacheService, TableService, MessageService, $modal,sweet) {
    $scope.configStr = "";
    $scope.envStr = "";

    $scope.getEnv = function(){
        AdminService.env({}, function(env){
            $scope.envStr = env.env;
            MessageService.sendMsg('Server environment get successfully', 'success', {});
//            sweet.show('Success!', 'Server environment get successfully', 'success');
        });
    }

    $scope.getConfig = function(){
        AdminService.config({}, function(config){
            $scope.configStr = config.config;
            MessageService.sendMsg('Server config get successfully', 'success', {});
        });
    }

    $scope.reloadMeta = function(){

        sweet.show({
            title: '',
            text: 'Are you sure to reload metadata and clean cache?',
            type: 'info',
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            confirmButtonText: "Yes",
            closeOnConfirm: false
        }, function() {
            CacheService.clean({}, function () {
                sweet.show('Success!', 'Cache reload successfully', 'success');
            });

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

    $scope.cleanStorage = function(){
        sweet.show({
            title: '',
            text: 'Are you sure to clean up unused HDFS and HBase space?',
            type: 'info',
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            confirmButtonText: "Yes",
            closeOnConfirm: false
        }, function() {
            AdminService.cleanStorage({}, function () {
                sweet.show('Success!', 'Storage cleaned successfully!', 'success');
            });
        });
    }

    $scope.disableCache = function(){
        sweet.show({
            title: '',
            text: 'Are you sure to disable query cache?',
            type: 'info',
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            confirmButtonText: "Yes",
            closeOnConfirm: false
        }, function() {
            AdminService.updateConfig({}, {key: 'kylin.query.cache.enabled',value:false}, function () {
                sweet.show('Success!', 'Cache disabled successfully!', 'success');
            });

        });

    }

    $scope.toSetConfig = function(){
        $modal.open({
            templateUrl: 'updateConfig.html',
            controller: updateConfigCtrl,
            resolve: {
            }
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
            sweet.show('Success!', 'A cardinality task has been submitted', 'success');
            TableService.genCardinality({tableName: $scope.tableName}, {delimiter: $scope.delimiter, format: $scope.format}, function (result) {
                MessageService.sendMsg('Cardinality job was calculated successfully. Click Refresh button ...', 'success', {});
            });
        }
    };

    var updateConfigCtrl = function ($scope, $modalInstance, AdminService, MessageService) {
        $scope.state = {
            key:null,
            value:null
        };
        $scope.cancel = function () {
            $modalInstance.dismiss('cancel');
        };
        $scope.update = function () {

            sweet.show({
                title: '',
                text: 'Are you sure to update config?',
                type: 'info',
                showCancelButton: true,
                confirmButtonColor: '#DD6B55',
                confirmButtonText: "Yes",
                closeOnConfirm: false
            }, function() {
                AdminService.updateConfig({}, {key: $scope.state.key, value: $scope.state.value}, function (result) {
                    sweet.show('Success!', 'Config updated successfully!', 'success');
                    $modalInstance.dismiss();
                });

            });

        }
    };

    $scope.getEnv();
    $scope.getConfig();
});