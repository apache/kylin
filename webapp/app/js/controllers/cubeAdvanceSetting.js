'use strict';

KylinApp.controller('CubeAdvanceSettingCtrl', function ($scope, $modal,cubeConfig,ModelService,MetaModel) {
    if($scope.state.mode==="edit") {
        $scope.metaModel = MetaModel;
    }
});
