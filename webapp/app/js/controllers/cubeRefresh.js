'use strict';

KylinApp.controller('CubeRefreshCtrl', function ($scope, $modal,cubeConfig,ModelService,MetaModel) {
    if(MetaModel.model.name){
        $scope.metaModel = MetaModel.getMetaModel();
    }
});
