/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/license$s/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

'use strict';

KylinApp.controller('CubeMeasuresCtrl', function ($scope, $modal,MetaModel,cubesManager,CubeDescModel) {

  $scope.addNewMeasure = function (measure) {
    $scope.newMeasure = (!!measure)? measure:CubeDescModel.createMeasure();
  };

  $scope.removeElement = function (arr, element) {
    var index = arr.indexOf(element);
    if (index > -1) {
      arr.splice(index, 1);
    }
  };

  $scope.clearNewMeasure = function () {
    $scope.newMeasure = null;
  };

  $scope.saveNewMeasure = function () {
    if ($scope.cubeMetaFrame.measures.indexOf($scope.newMeasure) === -1) {
      $scope.cubeMetaFrame.measures.push($scope.newMeasure);
    }
    $scope.newMeasure = null;
  };

  //map right return type for param
  $scope.measureReturnTypeUpdate = function(){
    if($scope.newMeasure.function.parameter.type=="constant"&&$scope.newMeasure.function.expression!=="COUNT_DISTINCT"){
      switch($scope.newMeasure.function.expression){
        case "SUM":
        case "COUNT":
          $scope.newMeasure.function.returntype = "bigint";
          break;
        default:
          $scope.newMeasure.function.returntype = "";
          break;
      }
    }
    if($scope.newMeasure.function.parameter.type=="column"&&$scope.newMeasure.function.expression!=="COUNT_DISTINCT"){

      var column = $scope.newMeasure.function.parameter.value;
      var colType = $scope.getColumnType(column, $scope.metaModel.model.fact_table); // $scope.getColumnType defined in cubeEdit.js

      if(colType==""||!colType){
        $scope.newMeasure.function.returntype = "";
        return;
      }


      switch($scope.newMeasure.function.expression){
        case "SUM":
          if(colType==="smallint"||colType==="int"||colType==="bigint"||colType==="integer"){
            $scope.newMeasure.function.returntype= 'bigint';
          }else{
            if(colType.indexOf('decimal')!=-1){
              $scope.newMeasure.function.returntype= colType;
            }else{
              $scope.newMeasure.function.returntype= 'decimal';
            }
          }
          break;
        case "MIN":
        case "MAX":
          $scope.newMeasure.function.returntype = colType;
          break;
        case "COUNT":
          $scope.newMeasure.function.returntype = "bigint";
          break;
        default:
          $scope.newMeasure.function.returntype = "";
          break;
      }
    }
  }


});
