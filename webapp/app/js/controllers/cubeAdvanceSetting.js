'use strict';

KylinApp.controller('CubeAdvanceSettingCtrl', function ($scope, $modal,cubeConfig,ModelService,MetaModel) {
    if(MetaModel.model.name){
        $scope.metaModel = MetaModel.getMetaModel();
    }
});
