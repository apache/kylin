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

KylinApp.controller('CubeAdvanceSettingCtrl', function ($scope, $modal,cubeConfig,MetaModel,cubesManager) {
    $scope.cubesManager = cubesManager;

    //convert some undefined or null value
    angular.forEach($scope.cubeMetaFrame.rowkey.rowkey_columns,function(rowkey){
            if(!rowkey.dictionary){
                rowkey.dictionary = "false";
            }
        }
    );
    //edit model
    if($scope.state.mode==="edit") {
        $scope.metaModel = MetaModel;
    }


    $scope.dictionaryUpdated = function(rowkey_column){
        if(rowkey_column.dictionary==="true"){
            rowkey_column.length=0;
        }

    }

  $scope.refreshAutoMergeTimeRanges = function(list,index,item){
    if (item) {
      list[index] = item;
    }
  }

});
