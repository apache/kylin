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

KylinApp.controller('CubeMeasuresCtrl', function ($scope, $modal,MetaModel,cubesManager,CubeDescModel,SweetAlert,VdmUtil,TableModel,cubeConfig,modelsManager,kylinConfig) {
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
  var needLengthKeyList=cubeConfig.needSetLengthEncodingList;
  $scope.getEncodings =function (name){
    var columnType = modelsManager.getColumnTypeByColumnName(name);
    var encodings =$scope.store.supportedEncoding,filterEncoding=[];
    var matchList=VdmUtil.getObjValFromLikeKey($scope.store.encodingMaps,columnType);
    if($scope.isEdit) {
      if (name && $scope.newMeasure.function.configuration&&$scope.newMeasure.function.configuration['topn.encoding.' + name]) {
        var version = $scope.newMeasure.function.configuration['topn.encoding_version.' + name] || 1;
        filterEncoding = VdmUtil.getFilterObjectListByOrFilterVal(encodings, 'value', $scope.newMeasure.function.configuration['topn.encoding.' + name].replace(/:\d+/, "") + (version ? "[v" + version + "]" : "[v1]"), 'suggest', true);
        matchList.push($scope.newMeasure.function.configuration['topn.encoding.' + name].replace(/:\d+/, ""));
        filterEncoding=VdmUtil.getObjectList(filterEncoding,'baseValue',matchList);
      }else{
        filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'suggest', true);
        filterEncoding=VdmUtil.getObjectList(filterEncoding,'baseValue',matchList);
      }
    }else{
      filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'suggest', true);
      filterEncoding=VdmUtil.getObjectList(filterEncoding,'baseValue',matchList);
    }
    return filterEncoding;
  }
  $scope.addNewMeasure = function (measure, index) {
    if(measure&&index>=0){
      $scope.updateMeasureStatus.isEdit = true;
      $scope.updateMeasureStatus.editIndex = index;
    }
    $scope.nextParameters = [];
    $scope.measureParamValueColumn=$scope.getCommonMetricColumns();
    $scope.newMeasure = (!!measure)? jQuery.extend(true, {},measure):CubeDescModel.createMeasure();
    if(!!measure && measure.function.parameter.next_parameter){
      $scope.nextPara.value = measure.function.parameter.next_parameter.value;
    }
    if($scope.newMeasure.function.parameter.value){
      if($scope.metaModel.model.metrics&&$scope.metaModel.model.metrics.indexOf($scope.newMeasure.function.parameter.value)!=-1){
          $scope.newMeasure.showDim=false;
      }else{
          $scope.newMeasure.showDim=true;
      }
    }else{
      $scope.newMeasure.showDim=false;
    }
    $scope.measureParamValueUpdate();
    if($scope.newMeasure.function.expression=="TOP_N"){
      $scope.convertedColumns=[];
      if($scope.newMeasure.function.configuration==null){
        var GroupBy = {
            name:$scope.newMeasure.function.parameter.next_parameter.value,
            encoding:"dict[v1]",
            valueLength:0,
            }
        $scope.convertedColumns.push(GroupBy);
      }
      for(var configuration in $scope.newMeasure.function.configuration) {
          if(/topn\.encoding\./.test(configuration)){
            var _name=configuration.slice(14);
            var item=$scope.newMeasure.function.configuration[configuration];
            var _encoding = item;
            var _valueLength = 0 ;
            var version=$scope.newMeasure.function.configuration['topn.encoding_version.'+_name]||1;
            item=$scope.removeVersion(item);
            var baseKey=item.replace(/:\d+/,'');
            if(needLengthKeyList.indexOf(baseKey)!=-1){
              var result=/:(\d+)/.exec(item);
              _valueLength=result?result[1]:0;
            }
            _encoding=baseKey;
            $scope.GroupBy = {
              name:_name,
              encoding: _encoding + (version ? "[v" + version + "]" : "[v1]"),
              valueLength:_valueLength,
              encoding_version:version||1
            }
            $scope.convertedColumns.push($scope.GroupBy);
        }
      };
    }
    if ($scope.newMeasure.function.expression === 'COUNT_DISTINCT') {
      $scope.convertedColumns=[];
      if ($scope.newMeasure.function.parameter.next_parameter) {
        $scope.recursion($scope.newMeasure.function.parameter.next_parameter, $scope.convertedColumns)
      }
    }
  };


  $scope.updateNextParameter = function(){
    for(var i= 0;i<$scope.nextParameters.length-1;i++){
      $scope.nextParameters[i].next_parameter=$scope.nextParameters[i+1];
    }
    $scope.newMeasure.function.parameter.next_parameter = $scope.nextParameters[0];
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
          var versionKey='topn.encoding_version.'+item.name;
          var version=$scope.getTypeVersion(item.encoding);
          var encoding="";
          if(needLengthKeyList.indexOf($scope.removeVersion(item.encoding))!=-1){
            encoding = $scope.removeVersion(item.encoding)+":"+item.valueLength;
          }else{
            encoding = $scope.removeVersion(item.encoding);
            item.valueLength=0;
          }
          $scope.newMeasure.function.configuration[a]= encoding;
          $scope.newMeasure.function.configuration[versionKey]=version;
          });
    }
    if ($scope.newMeasure.function.expression === 'COUNT_DISTINCT' && $scope.newMeasure.function.returntype!=='bitmap') {

      var hasExisted = [];

      for (var column in $scope.convertedColumns){
        if(hasExisted.indexOf($scope.convertedColumns[column].name)==-1){
          hasExisted.push($scope.convertedColumns[column].name);
        }
        else{
          SweetAlert.swal('', 'The column named ['+$scope.convertedColumns[column].name+'] already exits.', 'warning');
          return false;
        }
      }
      $scope.nextPara.next_parameter={};
      if ($scope.convertedColumns.length > 0) {
        $scope.groupby($scope.nextPara);
      } else {
        $scope.nextPara=null;
        $scope.newMeasure.function.parameter.next_parameter=null;
      }
    }

    if ($scope.isNameDuplicated($scope.cubeMetaFrame.measures, $scope.newMeasure) == true) {
      SweetAlert.swal('', 'The measure name: ' + $scope.newMeasure.name + ' is duplicated', 'warning');
      return false;
    }

    if($scope.nextPara && $scope.nextPara.value!=="" && ($scope.newMeasure.function.expression == 'EXTENDED_COLUMN' || $scope.newMeasure.function.expression == 'TOP_N'||$scope.newMeasure.function.expression == 'COUNT_DISTINCT')){
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
      encoding:"dict[v1]",
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

  $scope.recursion = function (parameter, list) {
    list.push({name: parameter.value})
    if (parameter.next_parameter) {
      $scope.recursion(parameter.next_parameter, list)
    } else {
      return
    }
  }

  $scope.measureParamValueUpdate = function(){
    if($scope.newMeasure.function.expression !== 'EXTENDED_COLUMN' && $scope.newMeasure.showDim==true){
       $scope.measureParamValueColumn=$scope.getAllModelDimMeasureColumns();
    }
    if($scope.newMeasure.function.expression !== 'EXTENDED_COLUMN' && $scope.newMeasure.showDim==false){
       $scope.measureParamValueColumn=$scope.getCommonMetricColumns();
    }
    if($scope.newMeasure.function.expression == 'EXTENDED_COLUMN'){
      $scope.measureParamValueColumn=$scope.getExtendedHostColumn();
    }
  }

  //map right return type for param
  $scope.measureReturnTypeUpdate = function() {

    if ($scope.newMeasure.function.expression == 'TOP_N') {
      if ($scope.newMeasure.function.parameter.type == "" || !$scope.newMeasure.function.parameter.type) {
        $scope.newMeasure.function.parameter.type = 'column';
      }
      $scope.convertedColumns = [];
      $scope.newMeasure.function.returntype = "topn(100)";
      return;
    } else if ($scope.newMeasure.function.expression == 'COUNT_DISTINCT') {
      $scope.newMeasure.function.parameter.type= 'column';
      $scope.newMeasure.function.returntype = "hllc(10)";
      $scope.convertedColumns = [];
    } else if($scope.newMeasure.function.expression == 'EXTENDED_COLUMN'){
      $scope.newMeasure.function.parameter.type= 'column';
      $scope.newMeasure.function.returntype = "extendedcolumn(100)";
      return;
    }else if($scope.newMeasure.function.expression=='PERCENTILE'){
      $scope.newMeasure.function.parameter.type= 'column';
    }else{
      $scope.nextParameterInit();
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
      $scope.newMeasure.function.returntype = $scope.getReturnType($scope.newMeasure.function.parameter.value, $scope.newMeasure.function.expression);
    }
  }

  // Open bulk add modal.
  $scope.openBulkAddModal = function () {

    $scope.initBulkAddMeasures();

    var modalInstance = $modal.open({
        templateUrl: 'bulkAddMeasures.html',
        controller: cubeBulkAddMeasureModalCtrl,
        backdrop: 'static',
        scope: $scope
    });
  };

    $scope.initBulkAddMeasures = function() {
    // init bulk add measure view model
    $scope.bulkMeasuresView = {
      SUM: [],
      MAX: [],
      MIN: [],
      RAW: [],
      PERCENTILE: []
    };
    angular.forEach($scope.getCommonMetricColumns(), function(paramValue) {
      var measures = _.filter($scope.cubeMetaFrame.measures, function(measure){ return measure.function.parameter.value == paramValue});
      for (var expression in $scope.bulkMeasuresView) {
        var bulkMeasure = {
          name: expression + '_' + paramValue.split('.')[1],
          parameter: paramValue,
          returntype: $scope.getReturnType(paramValue, expression),
          select: false,
          force: false
        };

        if (measures.length) {
          var measure = _.find(measures, function(measure){ return measure.function.expression == expression});
          if (!!measure) {
            bulkMeasure.name = measure.name;
            bulkMeasure.force = true;
            bulkMeasure.select = true;
          }
        }
        $scope.bulkMeasuresView[expression].push(bulkMeasure);
      }
    });

    // init expression selector
    $scope.bulkMeasureOptions = {
      expressionList: []
    };

    for (var expression in $scope.bulkMeasuresView) {
      var selectArr = _.filter($scope.bulkMeasuresView[expression], function(measure){ return measure.select && measure.force});
      var selectAll = $scope.getCommonMetricColumns().length == selectArr.length;
      var expressionSelect = {
        expression: expression,
        selectAll: selectAll,
        force: selectAll
      }
      $scope.bulkMeasureOptions.expressionList.push(expressionSelect);
    }

    $scope.bulkMeasureOptions.currentExpression = $scope.bulkMeasureOptions.expressionList[0];
  };

   $scope.getReturnType = function(parameter, expression) {
    if(parameter && (typeof parameter=="string")){
      var colType = $scope.getColumnType(VdmUtil.removeNameSpace(parameter), VdmUtil.getNameSpaceAliasName(parameter)); // $scope.getColumnType defined in cubeEdit.js
    }
    if(colType == '' || !colType) {
      return '';
    }

    switch(expression) {
      case 'SUM':
        if(colType === 'tinyint' || colType === 'smallint' || colType === 'int' || colType === 'bigint' || colType === 'integer') {
          return 'bigint';
        } else {
         if(colType.indexOf('decimal') != -1) {
            var returnRegex = new RegExp('(\\w+)(?:\\((\\w+?)(?:\\,(\\w+?))?\\))?');
            var returnValue = returnRegex.exec(colType);
            var precision = returnValue[2] || 0;
            var scale = returnValue[3] || 0;
            return 'decimal(' + precision + ',' + scale + ')';
          }else{
            return colType;
          }
        }
        break;
      case 'MIN':
      case 'MAX':
        return colType;
        break;
      case 'RAW':
        return 'raw';
        break;
      case 'COUNT':
        return 'bigint';
        break;
      case 'PERCENTILE':
        return 'percentile(100)';
        break;
      default:
        return '';
        break;
    }
  };

  if ($scope.state.mode == 'edit') {
    $scope.$on('$destroy', function () {
      // emit measures edit event in order to re-generate advanced dict.
      $scope.$emit('MeasuresEdited');
    });
  }

  $scope.isMeasureUnHidden = function(measure) {
    if (kylinConfig.getHiddenMeasures().indexOf(measure) == -1) {
      return true;
    } else {
      return false;
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

var cubeBulkAddMeasureModalCtrl = function($scope, $modalInstance, SweetAlert) {
  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.saveBulkMeasures = function () {
    $scope.bulkMeasures = [];
    // validation
    loopExp:
      for (var expression in $scope.bulkMeasuresView) {
        loopMeasureView:
          for (var measureView of $scope.bulkMeasuresView[expression]) {
            if (!measureView.force) {
              // validation
              var measureExisted = _.find($scope.cubeMetaFrame.measures, function(measure){ return measure.function.parameter.value == measureView.parameter && measure.function.expression == measureView.expression});
              if (!!measureExisted) {
                $scope.bulkMeasures = [];
                var errMsg = 'Duplicate measure for ' + measureView.name + ' and ' + measureExisted.name + '.';
                break loopExp;
              }
              if (measureView.select) {
                var measure = {
                  name: measureView.name,
                  function: {
                    expression: expression,
                    returntype: measureView.returntype,
                    parameter: {
                      type: 'column',
                      value: measureView.parameter
                    }
                  }
                };
                $scope.bulkMeasures.push(measure);
              }
            }
          }
      }
    if (!!errMsg) {
      SweetAlert.swal('', errMsg, 'warning');
    } else {
      $scope.cubeMetaFrame.measures = $scope.cubeMetaFrame.measures.concat($scope.bulkMeasures);
      $modalInstance.close();
    }
  };

  $scope.selectAll = function() {
    angular.forEach($scope.bulkMeasuresView[$scope.bulkMeasureOptions.currentExpression.expression], function(measure) {
      if (!measure.force) {
        measure.select = $scope.bulkMeasureOptions.currentExpression.selectAll;
      }
    });
  };

  $scope.measureChange = function(measureSelect) {
    if ($scope.bulkMeasureOptions.currentExpression.selectAll) {
      if (!measureSelect) {
        $scope.bulkMeasureOptions.currentExpression.selectAll = false;
      }
    } else {
      for(var measureView of $scope.bulkMeasuresView[$scope.bulkMeasureOptions.currentExpression.expression]) {
        if (!measureView.select) {
          return;
        }
      }
      $scope.bulkMeasureOptions.currentExpression.selectAll = true;
    }
  }
}
