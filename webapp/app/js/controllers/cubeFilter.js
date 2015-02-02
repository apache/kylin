'use strict';

KylinApp.controller('CubeFilterCtrl', function ($scope, $modal,cubeConfig,ModelService,MetaModel) {

    if(MetaModel.model.name){
        $scope.metaModel = MetaModel.getMetaModel();
    }
});
