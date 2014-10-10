'use strict';

KylinApp.controller('AdminCtrl', function ($scope,AdminService, CacheService, TableService, MessageService, $modal) {
    $scope.configStr = "";
    $scope.envStr = "";

    $scope.getEnv = function(){
        AdminService.env({}, function(env){
            $scope.envStr = env.env;
            MessageService.sendMsg('Server environment get successfully', 'success', {});
        });
    }

    $scope.getConfig = function(){
        AdminService.config({}, function(config){
            $scope.configStr = config.config;
            MessageService.sendMsg('Server config get successfully', 'success', {});
        });
    }

    $scope.reloadMeta = function(){
        if(confirm("Are you sure to reload metadata and clean cache?")) {
            CacheService.clean({}, function () {
                MessageService.sendMsg('Cache reload successfully', 'success', {});
            });
        }
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
        if(confirm("Are you sure to clean up unused HDFS and HBase space?")) {
            AdminService.cleanStorage({}, function () {
                MessageService.sendMsg('Storage cleaned successfully!', 'success', {});
            });
        }
    }

    $scope.disableCache = function(){
        if(confirm("Are you sure to disable query cache?")) {
            AdminService.updateConfig({}, {key: 'kylin.query.cache.enabled',value:false}, function () {
                MessageService.sendMsg('Cache disabled successfully!', 'success', {});
            });
        }
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
            MessageService.sendMsg('A cardinality task has been submitted.', 'success', {});
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
            if(confirm("Are you sure to update config?")) {
                AdminService.updateConfig({}, {key: $scope.state.key, value: $scope.state.value}, function (result) {
                    MessageService.sendMsg('Config updated successfully!', 'success', {});
                    $modalInstance.dismiss();
                });
            }
        }
    };

    $scope.getEnv();
    $scope.getConfig();
});