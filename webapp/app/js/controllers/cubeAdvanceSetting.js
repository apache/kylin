'use strict';

KylinApp.controller('CubeAdvanceSettingCtrl', function ($scope, $modal,cubeConfig,ModelService,MetaModel) {

    //convert some undefined or null value
    angular.forEach($scope.cubeMetaFrame.rowkey.rowkey_columns,function(rowkey){
            if(!rowkey.dictionary){
                rowkey.dictionary = "false";
            }
        }
    );
    console.log($scope.cubeMetaFrame.rowkey.rowkey_columns);

    if($scope.state.mode==="edit") {
        $scope.metaModel = MetaModel;
    }
});
