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

KylinApp.controller('CubeMeasuresCtrl', function ($scope, $modal,MetaModel,cubesManager,CubeDescModel,SweetAlert) {
  $scope.num=0;
  $scope.convertedColumns=[];
  $scope.groupby=[];
  $scope.initUpdateMeasureStatus = function(){
    $scope.updateMeasureStatus = {
      isEdit:false,
      editIndex:-1
    }
  };
  $scope.initUpdateMeasureStatus();

  $scope.addNewMeasure = function (measure, index) {
    if(measure&&index>=0){
      $scope.updateMeasureStatus.isEdit = true;
      $scope.updateMeasureStatus.editIndex = index;
    }
    $scope.nextParameters = [];
    $scope.newMeasure = (!!measure)? jQuery.extend(true, {},measure):CubeDescModel.createMeasure();
    if(!!measure && measure.function.parameter.next_parameter){
      $scope.nextPara.value = measure.function.parameter.next_parameter.value;
    }
    if($scope.newMeasure.function.expression=="TOP_N"){
      $scope.convertedColumns=[];
      if($scope.newMeasure.function.configuration==null){
        var GroupBy = {
            name:$scope.newMeasure.function.parameter.next_parameter.value,
            encoding:"dict",
            valueLength:0,
            }
        $scope.convertedColumns.push(GroupBy);
      }
      for(var configuration in $scope.newMeasure.function.configuration) {
        var _name=configuration.slice(14);
        var item=$scope.newMeasure.function.configuration[configuration];
        var _isFixedLength = item.substring(0,12) === "fixed_length"?"true":"false";//fixed_length:12
        var _isIntegerLength = item.substring(0,7) === "integer"?"true":"false";
        var _isIntLength = item.substring(0,3) === "int"?"true":"false";
        var _encoding = item;
        var _valueLength = 0 ;
        if(_isFixedLength !=="false"){
          _valueLength = item.substring(13,item.length);
          _encoding = "fixed_length";
        }
        if(_isIntLength!="false" && _isIntegerLength=="false" ){
          _valueLength = item.substring(4,item.length);
          _encoding = "int";
        }

        if(_isIntegerLength!="false" ){
          _valueLength = item.substring(8,item.length);
          _encoding = "integer";
        }

        $scope.GroupBy = {
          name:_name,
          encoding:_encoding,
          valueLength:_valueLength,
        }
        $scope.convertedColumns.push($scope.GroupBy);
      };
    }
  };


  $scope.updateNextParameter = function(){
    for(var i= 0;i<$scope.nextParameters.length-1;i++){
      $scope.nextParameters[i].next_parameter=$scope.nextParameters[i+1];
    }
    $scope.newMeasure.function.parameter.next_parameter = $scope.nextParameters[0];
    console.log($scope.newMeasure.function.parameter);
  }

  $scope.editNextParameter = function(parameter){
    $scope.openParameterModal(parameter);

  }

  $scope.addNextParameter = function(){
    $scope.openParameterModal();
  }

  $scope.removeParameter = function(parameters,index){
    if(index>-1){
      parameters.splice(index,1);
    }
    $scope.updateNextParameter();
  }


  $scope.nextPara = {
    "type":"column",
    "value":"",
    "next_parameter":null
  }

  $scope.openParameterModal = function (parameter) {
    $modal.open({
      templateUrl: 'nextParameter.html',
      controller: NextParameterModalCtrl,
      resolve: {
        scope: function () {
          return $scope;
        },
        para:function(){
          return parameter;
        }
      }
    });
  };
  $scope.nextParameters =[];

  $scope.removeElement = function (arr, element) {
    var index = arr.indexOf(element);
    if (index > -1) {
      arr.splice(index, 1);
    }
  };

  $scope.clearNewMeasure = function () {
    $scope.newMeasure = null;
    $scope.initUpdateMeasureStatus();
    $scope.nextParameterInit();
  };

  $scope.saveNewMeasure = function () {

    if ($scope.newMeasure.function.expression === 'TOP_N' ) {
      if($scope.newMeasure.function.parameter.value == ""){
        SweetAlert.swal('', '[TOP_N] ORDER|SUM by Column  is required', 'warning');
        return false;
      }
      if($scope.convertedColumns.length<1){
        SweetAlert.swal('', '[TOP_N] Group by Column is required', 'warning');
        return false;
      }

      var hasExisted = [];

      for (var column in $scope.convertedColumns){
        if(hasExisted.indexOf($scope.convertedColumns[column].name)==-1){
          hasExisted.push($scope.convertedColumns[column].name);
        }
        else{
          SweetAlert.swal('', 'The column named ['+$scope.convertedColumns[column].name+'] already exits.', 'warning');
          return false;
        }
        if ($scope.convertedColumns[column].encoding == 'int' && ($scope.convertedColumns[column].valueLength < 1 || $scope.convertedColumns[column].valueLength > 8)) {
          SweetAlert.swal('', 'int encoding column length should between 1 and 8.', 'warning');
          return false;
        }
      }
        $scope.nextPara.next_parameter={};
        $scope.newMeasure.function.configuration={};
        $scope.groupby($scope.nextPara);
        angular.forEach($scope.convertedColumns,function(item){
          var a='topn.encoding.'+item.name;
          var encoding="";
          if(item.encoding!=="dict" && item.encoding!=="date"&& item.encoding!=="time"){
            if(item.encoding=="fixed_length" && item.valueLength){
              encoding = "fixed_length:"+item.valueLength;
            }else if(item.encoding=="integer" && item.valueLength){
              encoding = "integer:"+item.valueLength;
            }else if(item.encoding=="int" && item.valueLength){
              encoding = "int:"+item.valueLength;
            }else{
              encoding = item.encoding;
            }
          }else{
            encoding = item.encoding;
            item.valueLength=0;
          }
          $scope.newMeasure.function.configuration[a]=encoding;
          });
    }

    if ($scope.isNameDuplicated($scope.cubeMetaFrame.measures, $scope.newMeasure) == true) {
      SweetAlert.swal('', 'The measure name: ' + $scope.newMeasure.name + ' is duplicated', 'warning');
      return false;
    }

    if($scope.nextPara.value!=="" && ($scope.newMeasure.function.expression == 'EXTENDED_COLUMN' || $scope.newMeasure.function.expression == 'TOP_N')){
      $scope.newMeasure.function.parameter.next_parameter = jQuery.extend(true,{},$scope.nextPara);
    }

    if($scope.updateMeasureStatus.isEdit == true){
      $scope.cubeMetaFrame.measures[$scope.updateMeasureStatus.editIndex] = $scope.newMeasure;
    }
    else {
      $scope.cubeMetaFrame.measures.push($scope.newMeasure);
    }

    $scope.newMeasure = null;
    $scope.initUpdateMeasureStatus();
    $scope.nextParameterInit();
    return true;
  };

  $scope.isNameDuplicated = function (measures, newMeasure) {
    var names = [];
    for(var i = 0;i < measures.length; i++){
        names.push(measures[i].name);
    }
    var index = names.indexOf(newMeasure.name);
    return (index > -1 && index != $scope.updateMeasureStatus.editIndex);
  }

  $scope.nextParameterInit = function(){
    $scope.nextPara = {
      "type":"column",
      "value":"",
      "next_parameter": null
    }
    if($scope.newMeasure){
      $scope.newMeasure.function.parameter.next_parameter = null;
    }
  }

  $scope.addNewGroupByColumn = function () {
    $scope.nextGroupBy = {
      name:null,
      encoding:"dict",
      valueLength:0,
    }
    $scope.convertedColumns.push($scope.nextGroupBy);

  };

  $scope.removeColumn = function(arr,index){
    if (index > -1) {
      arr.splice(index, 1);
    }
  };
  $scope.refreshGroupBy=function (list,index,item) {
    var encoding;
    var name = item.name;
    if(item.encoding=="dict" || item.encoding=="date"|| item.encoding=="time"){
      item.valueLength=0;
    }
  };

  $scope.groupby= function (next_parameter){
    if($scope.num<$scope.convertedColumns.length-1){
      next_parameter.value=$scope.convertedColumns[$scope.num].name;
      next_parameter.type="column";
      next_parameter.next_parameter={};
      $scope.num++;
      $scope.groupby(next_parameter.next_parameter);
    }
    else{
      next_parameter.value=$scope.convertedColumns[$scope.num].name;
      next_parameter.type="column";
      next_parameter.next_parameter=null;
      $scope.num=0;
      return false;
    }
  }
  $scope.converted = function (next_parameter) {
    if (next_parameter != null) {
      $scope.groupby.push(next_parameter.value);
      converted(next_parameter.next_parameter)
    }
    else {
      $scope.groupby.push(next_parameter.value);
      return false;
    }
  }
  //map right return type for param
  $scope.measureReturnTypeUpdate = function(){

    if($scope.newMeasure.function.expression == 'TOP_N'){
      if($scope.newMeasure.function.parameter.type==""||!$scope.newMeasure.function.parameter.type){
        $scope.newMeasure.function.parameter.type= 'column';
      }
      $scope.convertedColumns=[];
      $scope.newMeasure.function.returntype = "topn(100)";
      return;
    }else if($scope.newMeasure.function.expression == 'EXTENDED_COLUMN'){
      $scope.newMeasure.function.parameter.type= 'column';
      $scope.newMeasure.function.returntype = "extendedcolumn(100)";
      return;
    }else{
      $scope.nextParameterInit();
    }

    if($scope.newMeasure.function.expression == 'COUNT'){
      $scope.newMeasure.function.parameter.type= 'constant';
    }

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
        case "RAW":
          $scope.newMeasure.function.returntype = "raw";
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

var NextParameterModalCtrl = function ($scope, scope,para,$modalInstance,cubeConfig, CubeService, MessageService, $location, SweetAlert,ProjectModel, loadingRequest,ModelService) {

  $scope.newmea={
    "measure":scope.newMeasure
  }

  $scope.cubeConfig = cubeConfig;
  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.getCommonMetricColumns = function(measure){
    return scope.getCommonMetricColumns(measure);
  }

  $scope.nextPara = {
    "type":"",
    "value":"",
    "next_parameter":null
  }

  var _index = scope.nextParameters.indexOf(para);
  if(para){
    $scope.nextPara = para;
  }

  $scope.ok = function(){
    if(_index!=-1){
      scope.nextParameters[_index] = $scope.nextPara;
    }
    else{
      scope.nextParameters.push($scope.nextPara);
    }

    scope.updateNextParameter();
    $modalInstance.dismiss('cancel');
  }

}
