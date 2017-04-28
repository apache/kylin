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

KylinApp
  .controller('SourceMetaCtrl', function ($scope, $cacheFactory, $q, $window, $routeParams, CubeService, $modal, TableService, $route, loadingRequest, SweetAlert, tableConfig, TableModel,cubeConfig) {
    var $httpDefaultCache = $cacheFactory.get('$http');
    $scope.tableModel = TableModel;
    $scope.tableModel.selectedSrcDb = [];
    $scope.tableModel.selectedSrcTable = {};
    $scope.window = 0.68 * $window.innerHeight;
    $scope.tableConfig = tableConfig;
    $scope.isCalculate = true;

    $scope.state = {
      filterAttr: 'id', filterReverse: false, reverseColumn: 'id',
      dimensionFilter: '', measureFilter: ''
    };

    function innerSort(a, b) {
      var nameA = a.name.toLowerCase(), nameB = b.name.toLowerCase();
      if (nameA < nameB) //sort string ascending
        return -1;
      if (nameA > nameB)
        return 1;
      return 0; //default return value (no sorting)
    };

    $scope.aceSrcTbLoaded = function (forceLoad) {
      //stop request when project invalid
      if (!$scope.projectModel.getSelectedProject()) {
        TableModel.init();
        return;
      }
      if (forceLoad) {
        $httpDefaultCache.removeAll();
      }
      $scope.loading = true;
      TableModel.aceSrcTbLoaded(forceLoad).then(function () {
        $scope.loading = false;
      });
    };

    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
//         will load table when enter this page,null or not
      $scope.aceSrcTbLoaded();
    }, function (resp) {
      SweetAlert.swal('Oops...', resp, 'error');
    });


    $scope.showSelected = function (obj) {
      if (obj.uuid) {
        $scope.tableModel.selectedSrcTable = obj;
      }
      else if (obj.datatype) {
        $scope.tableModel.selectedSrcTable.selectedSrcColumn = obj;
      }
    };

    $scope.aceSrcTbChanged = function () {
      $scope.tableModel.selectedSrcDb = [];
      $scope.tableModel.selectedSrcTable = {};
      $scope.aceSrcTbLoaded(true);
    };


    $scope.openModal = function () {
      if(!$scope.projectModel.selectedProject){
        SweetAlert.swal('Oops...', "Please select a project.", 'info');
        return;
      }
      $modal.open({
        templateUrl: 'addHiveTable.html',
        controller: ModalInstanceCtrl,
        backdrop : 'static',
        resolve: {
          tableNames: function () {
            return $scope.tableNames;
          },
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          isCalculate: function () {
            return $scope.isCalculate;
          },
          scope: function () {
            return $scope;
          }
        }
      });
    };

    $scope.openTreeModal = function () {
      if(!$scope.projectModel.selectedProject){
        SweetAlert.swal('Oops...', "Please select a project.", 'info');
        return;
      }
      $modal.open({
        templateUrl: 'addHiveTableFromTree.html',
        controller: ModalInstanceCtrl,
        resolve: {
          tableNames: function () {
            return $scope.tableNames;
          },
          projectName:function(){
            return  $scope.projectModel.selectedProject;
          },
          isCalculate: function () {
            return $scope.isCalculate;
          },
          scope: function () {
            return $scope;
          }
        }
      });
    };

    $scope.reloadTable = function (tableName){
      var delay = $q.defer();
      loadingRequest.show();
      TableService.loadHiveTable({tableName: tableName, action: $scope.projectModel.selectedProject}, {calculate: $scope.isCalculate}, function (result) {
        var loadTableInfo = "";
        angular.forEach(result['result.loaded'], function (table) {
          loadTableInfo += "\n" + table;
        })
        var unloadedTableInfo = "";
        angular.forEach(result['result.unloaded'], function (table) {
          unloadedTableInfo += "\n" + table;
        })
        if (result['result.unloaded'].length != 0 && result['result.loaded'].length == 0) {
          SweetAlert.swal('Failed!', 'Failed to load following table(s): ' + unloadedTableInfo, 'error');
        }
        if (result['result.loaded'].length != 0 && result['result.unloaded'].length == 0) {
          SweetAlert.swal('Success!', 'The following table(s) have been successfully loaded: ' + loadTableInfo, 'success');
        }
        if (result['result.loaded'].length != 0 && result['result.unloaded'].length != 0) {
          SweetAlert.swal('Partial loaded!', 'The following table(s) have been successfully loaded: ' + loadTableInfo + "\n\n Failed to load following table(s):" + unloadedTableInfo, 'warning');
        }
        loadingRequest.hide();
        delay.resolve("");
      }, function (e) {
        if (e.data && e.data.exception) {
          var message = e.data.exception;
          var msg = !!(message) ? message : 'Failed to take action.';
          SweetAlert.swal('Oops...', msg, 'error');
        } else {
          SweetAlert.swal('Oops...', "Failed to take action.", 'error');
        }
        loadingRequest.hide();
      })
       return delay.promise;
    }



    $scope.unloadTable = function (tableName) {
      SweetAlert.swal({
            title: "",
            text: "Are you sure to unload this table?",
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            confirmButtonText: "Yes",
            cancelButtonText: "No",
            closeOnConfirm: true
      }, function (isConfirm) {
        if (isConfirm) {
          if (!$scope.projectModel.selectedProject) {
            SweetAlert.swal('', 'Please choose your project first!.', 'info');
            return;
          }
          loadingRequest.show();
          TableService.unLoadHiveTable({tableName: tableName, action: $scope.projectModel.selectedProject}, {}, function (result) {
            var removedTableInfo = "";
            angular.forEach(result['result.unload.success'], function (table) {
              removedTableInfo += "\n" + table;
            })
            var unRemovedTableInfo = "";
            angular.forEach(result['result.unload.fail'], function (table) {
              unRemovedTableInfo += "\n" + table;
            })
            if (result['result.unload.fail'].length != 0 && result['result.unload.success'].length == 0) {
              SweetAlert.swal('Failed!', 'Failed to unload following table(s): ' + unRemovedTableInfo, 'error');
            }
            if (result['result.unload.success'].length != 0 && result['result.unload.fail'].length == 0) {
              SweetAlert.swal('Success!', 'The following table(s) have been successfully unloaded: ' + removedTableInfo, 'success');
            }
            if (result['result.unload.success'].length != 0 && result['result.unload.fail'].length != 0) {
              SweetAlert.swal('Partial unloaded!', 'The following table(s) have been successfully unloaded: ' + removedTableInfo + "\n\n Failed to unload following table(s):" + unRemovedTableInfo, 'warning');
            }
            loadingRequest.hide();
            $scope.aceSrcTbLoaded(true);
          }, function (e) {
            if (e.data && e.data.exception) {
              var message = e.data.exception;
              var msg = !!(message) ? message : 'Failed to take action.';
              SweetAlert.swal('Oops...', msg, 'error');
            } else {
              SweetAlert.swal('Oops...', "Failed to take action.", 'error');
            }
            loadingRequest.hide();
          })
        }
      })
    }

    var ModalInstanceCtrl = function ($scope, $location, $modalInstance, tableNames, MessageService, projectName, isCalculate, scope, kylinConfig) {
      $scope.tableNames = "";
      $scope.projectName = projectName;
      $scope.isCalculate = isCalculate;

      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };

      $scope.kylinConfig = kylinConfig;


      $scope.treeOptions = {multiSelection: true};
      $scope.selectedNodes = [];
      $scope.hiveLimit =  kylinConfig.getHiveLimit();

      $scope.loadHive = function () {
        if($scope.hiveLoaded)
          return;
        TableService.showHiveDatabases({}, function (databases) {
          $scope.dbNum = databases.length;
          if (databases.length > 0) {
            $scope.hiveMap = {};
            for (var i = 0; i < databases.length; i++) {
              var dbName = databases[i];
              var hiveData = {"dbname":dbName,"tables":[],"expanded":false};
              $scope.hive.push(hiveData);
              $scope.hiveMap[dbName] = i;
            }
          }
          $scope.hiveLoaded = true;
          $scope.showMoreDatabases();
        });
      }

      $scope.showMoreTables = function(hiveTables, node){
        var shownTimes = parseInt(node.children.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = 0;
        var hasMore = false;
        if(from + $scope.hiveLimit > hiveTables.length) {
          to = hiveTables.length - 1;
        } else {
          to = from + $scope.hiveLimit - 1;
          hasMore = true;
        }
        if(!angular.isUndefined(node.children[from])){
          node.children.pop();
        }

        for(var idx = from; idx <= to; idx++){
          node.children.push({"label":node.label+'.'+hiveTables[idx],"id":idx-from+1,"children":[]});
        }

        if(hasMore){
          var loading = {"label":"","id":65535,"children":[]};
          node.children.push(loading);
        }
      }

      $scope.showAllTables = function(hiveTables, node){
        var shownTimes = parseInt(node.children.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = hiveTables.length - 1;
        if(!angular.isUndefined(node.children[from])){
          node.children.pop();
        }
        for(var idx = from; idx <= to; idx++){
          node.children.push({"label":node.label+'.'+hiveTables[idx],"id":idx-from+1,"children":[]});
        }
      }

      $scope.showMoreDatabases = function(){
        var shownTimes = parseInt($scope.treedata.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = 0;
        var hasMore = false;
        if(from + $scope.hiveLimit > $scope.hive.length) {
          to = $scope.hive.length - 1;
        } else {
          to = from + $scope.hiveLimit - 1;
          hasMore = true;
        }
        if(!angular.isUndefined($scope.treedata[from])){
          $scope.treedata.pop();
        }

        for(var idx = from; idx <= to; idx++){
          var children = [];
          var loading = {"label":"","id":0,"children":[]};
          children.push(loading);
          $scope.treedata.push({"label":$scope.hive[idx].dbname,"id":idx+1,"children":children,"expanded":false});
        }

        if(hasMore){
          var loading = {"label":"","id":65535,"children":[0]};
          $scope.treedata.push(loading);
        }
      }

      $scope.showAllDatabases = function(){
        var shownTimes = parseInt($scope.treedata.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = $scope.hive.length - 1;

        if(!angular.isUndefined($scope.treedata[from])){
          $scope.treedata.pop();
        }

        for(var idx = from; idx <= to; idx++){
          var children = [];
          var loading = {"label":"","id":0,"children":[]};
          children.push(loading);
          $scope.treedata.push({"label":$scope.hive[idx].dbname,"id":idx+1,"children":children,"expanded":false});
        }
      }

      $scope.showMoreClicked = function($parentNode){
        if($parentNode == null){
          $scope.showMoreDatabases();
        } else {
          $scope.showMoreTables($scope.hive[$scope.hiveMap[$parentNode.label]].tables,$parentNode);
        }
      }

      $scope.showAllClicked = function($parentNode){
        if($parentNode == null){
          $scope.showAllDatabases();
        } else {
          $scope.showAllTables($scope.hive[$scope.hiveMap[$parentNode.label]].tables,$parentNode);
        }
      }

      $scope.showToggle = function(node) {
        if(node.expanded == false){
          TableService.showHiveTables({"database": node.label},function (hive_tables){
            var tables = [];
            for (var i = 0; i < hive_tables.length; i++) {
              tables.push(hive_tables[i]);
            }
            $scope.hive[$scope.hiveMap[node.label]].tables = tables;
            $scope.showMoreTables(tables,node);
            node.expanded = true;
          });
        }
      }

      $scope.showSelected = function(node) {

      }

      if(angular.isUndefined($scope.hive) || angular.isUndefined($scope.hiveLoaded) || angular.isUndefined($scope.treedata) ){
        $scope.hive = [];
        $scope.hiveLoaded = false;
        $scope.treedata = [];
        $scope.loadHive();
      }




      $scope.add = function () {

        if($scope.tableNames.length === 0 && $scope.selectedNodes.length > 0) {
          for(var i = 0; i <  $scope.selectedNodes.length; i++){
            if($scope.selectedNodes[i].label.indexOf(".") >= 0){
              $scope.tableNames += ($scope.selectedNodes[i].label) += ',';
            }
          }
        }

        if ($scope.tableNames.trim() === "") {
          SweetAlert.swal('', 'Please input table(s) you want to load.', 'info');
          return;
        }

        if (!$scope.projectName) {
          SweetAlert.swal('', 'Please choose your project first!.', 'info');
          return;
        }

        $scope.cancel();
        scope.reloadTable($scope.tableNames).then(function(){
             scope.aceSrcTbLoaded(true);
           });
      }



    };

    $scope.editStreamingConfig = function(tableName){
      var modalInstance = $modal.open({
        templateUrl: 'editStreamingSource.html',
        controller: EditStreamingSourceCtrl,
        backdrop : 'static',
        resolve: {
          tableNames: function () {
            return $scope.tableNames;
          },
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          tableName: function(){
            return tableName;
          },
          scope: function () {
            return $scope;
          }
        }
      });

      modalInstance.result.then(function () {
        $scope.$broadcast('StreamingConfigEdited');
      }, function () {
        $scope.$broadcast('StreamingConfigEdited');
      });


    }

    //streaming model
    $scope.openStreamingSourceModal = function () {
      if(!$scope.projectModel.selectedProject){
        SweetAlert.swal('Oops...', "Please select a project.", 'info');
        return;
      }
      $modal.open({
        templateUrl: 'addStreamingSource.html',
        controller: StreamingSourceCtrl,
        backdrop : 'static',
        resolve: {
          tableNames: function () {
            return $scope.tableNames;
          },
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          scope: function () {
            return $scope;
          }
        }
      });
    };

    var EditStreamingSourceCtrl = function ($scope, $interpolate, $templateCache, tableName, $modalInstance, tableNames, MessageService, projectName, scope, tableConfig,cubeConfig,StreamingModel,StreamingService) {

      $scope.state = {
        tableName : tableName,
        mode: "edit",
        target:"kfkConfig"
      }

      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };

      $scope.projectName = projectName;
      $scope.streamingMeta = StreamingModel.createStreamingConfig();
      $scope.kafkaMeta = StreamingModel.createKafkaConfig();
      $scope.updateStreamingMeta = function(val){
        $scope.streamingMeta = val;
      }
      $scope.updateKafkaMeta = function(val){
        $scope.kafkaMeta = val;
      }

      $scope.streamingResultTmpl = function (notification) {
        // Get the static notification template.
        var tmpl = notification.type == 'success' ? 'streamingResultSuccess.html' : 'streamingResultError.html';
        return $interpolate($templateCache.get(tmpl))(notification);
      };

      $scope.updateStreamingSchema = function(){
        StreamingService.update({}, {
          project: $scope.projectName,
          tableData:angular.toJson(""),
          streamingConfig: angular.toJson($scope.streamingMeta),
          kafkaConfig: angular.toJson($scope.kafkaMeta)
        }, function (request) {
          if (request.successful) {
            SweetAlert.swal('', 'Updated the streaming successfully.', 'success');
            $scope.cancel();
          } else {
            var message = request.message;
            var msg = !!(message) ? message : 'Failed to take action.';
            MessageService.sendMsg($scope.streamingResultTmpl({
              'text': msg,
              'streamingSchema': angular.toJson($scope.streamingMeta,true),
              'kfkSchema': angular.toJson($scope.kafkaMeta,true)
            }), 'error', {}, true, 'top_center');
          }
          loadingRequest.hide();
        }, function (e) {
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Failed to take action.';
            MessageService.sendMsg($scope.streamingResultTmpl({
              'text': msg,
              'streamingSchema': angular.toJson($scope.streamingMeta,true),
              'kfkSchema': angular.toJson($scope.kafkaMeta,true)
            }), 'error', {}, true, 'top_center');
          } else {
            MessageService.sendMsg($scope.streamingResultTmpl({
              'text': msg,
              'streamingSchema': angular.toJson($scope.streamingMeta,true),
              'kfkSchema': angular.toJson($scope.kafkaMeta,true)
            }), 'error', {}, true, 'top_center');
          }
          //end loading
          loadingRequest.hide();

        })
      }

    }

    var StreamingSourceCtrl = function ($scope, $location,$interpolate,$templateCache, $modalInstance, tableNames, MessageService, projectName, scope, tableConfig,cubeConfig,StreamingModel,StreamingService) {

      $scope.state={
        'mode':'edit'
      }

      $scope.streamingMeta = StreamingModel.createStreamingConfig();
      $scope.kafkaMeta = StreamingModel.createKafkaConfig();



      $scope.steps = {
        curStep:1
      };

      $scope.streamingCfg = {
        parseTsColumn:"{{}}",
        columnOptions:[]
      }

      $scope.previewStep = function(){
        $scope.steps.curStep--;
      }

      $scope.nextStep = function(){

        $scope.checkFailed = false;

        //check form
        $scope.form['setStreamingSchema'].$submitted = true;
        if(!$scope.streaming.sourceSchema||$scope.streaming.sourceSchema===""){
          $scope.checkFailed = true;
        }

        if(!$scope.table.name||$scope.table.name===""){
          $scope.checkFailed = true;
        }

        $scope.prepareNextStep();

        if(!$scope.rule.timestampColumnExist){
          $scope.checkFailed = true;
        }

        if($scope.checkFailed){
          return;
        }

        $scope.steps.curStep++;
      }

      $scope.prepareNextStep = function(){
        $scope.streamingCfg.columnOptions = [];
        $scope.rule.timestampColumnExist = false;
        angular.forEach($scope.columnList,function(column,$index){
          if (column.checked == "Y" && column.fromSource=="Y" && column.type == "timestamp") {
            $scope.streamingCfg.columnOptions.push(column.name);
            $scope.rule.timestampColumnExist = true;
          }
        })

        if($scope.streamingCfg.columnOptions.length==1){
          $scope.streamingCfg.parseTsColumn = $scope.streamingCfg.columnOptions[0];
          $scope.kafkaMeta.parserProperties = "tsColName="+$scope.streamingCfg.parseTsColumn;
        }
        if($scope.kafkaMeta.parserProperties!==''){
          $scope.state.isParserHeaderOpen = false;
        }else{
          $scope.state.isParserHeaderOpen = true;
        }
      }

      $scope.projectName = projectName;
      $scope.tableConfig = tableConfig;
      $scope.cubeConfig = cubeConfig;
      $scope.streaming = {
        sourceSchema: '',
        'parseResult': {}
      }

      $scope.table = {
        name: '',
        sourceValid:false,
        schemaChecked:false
      }

      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };

      $scope.streamingOnLoad = function () {
        console.log($scope.streaming.sourceSchema);
      }

      $scope.columnList = [];

      $scope.streamingOnChange = function () {
        $scope.table.schemaChecked = true;
        console.log($scope.streaming.sourceSchema);
        try {
          $scope.streaming.parseResult = JSON.parse($scope.streaming.sourceSchema);
        } catch (error) {
          $scope.table.sourceValid = false;
          console.log(error);
          return;
        }
        $scope.table.sourceValid = true;

        //streaming table data change structure
        var columnList=[]
        function changeObjTree(obj,base){
          base=base?base+"_":"";
          for(var i in obj){
            if(Object.prototype.toString.call(obj[i])=="[object Object]"){
              changeObjTree(obj[i],base+i);
              continue;
            }
            columnList.push(createNewObj(base+i,obj[i]));
          }
        }

        function checkValType(val,key){
          var defaultType;
          if(typeof val ==="number"){
              if(/id/i.test(key)&&val.toString().indexOf(".")==-1){
                defaultType="int";
              }else if(val <= 2147483647){
                if(val.toString().indexOf(".")!=-1){
                  defaultType="decimal";
                }else{
                  defaultType="int";
                }
              }else{
                defaultType="timestamp";
              }
          }else if(typeof val ==="string"){
              if(!isNaN((new Date(val)).getFullYear())&&typeof ((new Date(val)).getFullYear())==="number"){
                defaultType="date";
              }else{
                defaultType="varchar(256)";
              }
          }else if(Object.prototype.toString.call(val)=="[object Array]"){
              defaultType="varchar(256)";
          }else if (typeof val ==="boolean"){
              defaultType="boolean";
          }
          return defaultType;
        }

        function createNewObj(key,val){
          var obj={};
          obj.name=key;
          obj.type=checkValType(val,key);
          obj.fromSource="Y";
          obj.checked="Y";
          if(Object.prototype.toString.call(val)=="[object Array]"){
            obj.checked="N";
          }
          return obj;
        }
        changeObjTree($scope.streaming.parseResult);
        var timeMeasure = $scope.cubeConfig.streamingAutoGenerateMeasure;
        for(var i = 0;i<timeMeasure.length;i++){
          var defaultCheck = 'Y';
          columnList.push({
            'name': timeMeasure[i].name,
            'checked': defaultCheck,
            'type': timeMeasure[i].type,
            'fromSource':'N'
          });
        }

        var firstCommit = false;
        if($scope.columnList.length==0){
          firstCommit = true;
        }

        if(!firstCommit){
          angular.forEach(columnList,function(item){
            for(var i=0;i<$scope.columnList.length;i++){
              if($scope.columnList[i].name==item.name){
                item.checked = $scope.columnList[i].checked;
                item.type = $scope.columnList[i].type;
                item.fromSource = $scope.columnList[i].fromSource;
                break;
              }
            }
          })
        }
        $scope.columnList = columnList;
      }


      $scope.streamingResultTmpl = function (notification) {
        // Get the static notification template.
        var tmpl = notification.type == 'success' ? 'streamingResultSuccess.html' : 'streamingResultError.html';
        return $interpolate($templateCache.get(tmpl))(notification);
      };


      $scope.form={};
      $scope.rule={
        'timestampColumnExist':false
      }

      $scope.modelMode == "addStreaming";

      $scope.syncStreamingSchema = function () {

        $scope.form['cube_streaming_form'].$submitted = true;

        if($scope.form['cube_streaming_form'].parserName.$invalid || $scope.form['cube_streaming_form'].parserProperties.$invalid) {
          $scope.state.isParserHeaderOpen = true;
        }

        if($scope.form['cube_streaming_form'].$invalid){
            return;
        }

        var columns = [];
        angular.forEach($scope.columnList,function(column,$index){
          if (column.checked == "Y") {
            var columnInstance = {
              "id": ++$index,
              "name": column.name,
              "datatype": column.type
            }
            columns.push(columnInstance);
          }
        })


        $scope.tableData = {
          "name": $scope.table.name,
          "source_type":1,
          "columns": columns,
          'database':'Default'
        }


        $scope.kafkaMeta.name = $scope.table.name
        $scope.streamingMeta.name = $scope.table.name;

        SweetAlert.swal({
          title:"",
          text: 'Are you sure to save the streaming table and cluster info ?',
          showCancelButton: true,
          confirmButtonColor: '#DD6B55',
          confirmButtonText: "Yes",
          closeOnConfirm: true
        }, function (isConfirm) {
          if (isConfirm) {
            loadingRequest.show();

            if ($scope.modelMode == "editExistStreaming") {
              StreamingService.update({}, {
                project: $scope.projectName,
                tableData:angular.toJson($scope.tableData),
                streamingConfig: angular.toJson($scope.streamingMeta),
                kafkaConfig: angular.toJson($scope.kafkaMeta)
              }, function (request) {
                if (request.successful) {
                  SweetAlert.swal('', 'Updated the streaming successfully.', 'success');
                  $location.path("/models");
                } else {
                  var message = request.message;
                  var msg = !!(message) ? message : 'Failed to take action.';
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta,true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta,true)
                  }), 'error', {}, true, 'top_center');
                }
                loadingRequest.hide();
              }, function (e) {
                if (e.data && e.data.exception) {
                  var message = e.data.exception;
                  var msg = !!(message) ? message : 'Failed to take action.';
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta,true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta,true)
                  }), 'error', {}, true, 'top_center');
                } else {
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta,true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta,true)
                  }), 'error', {}, true, 'top_center');
                }
                //end loading
                loadingRequest.hide();

              })
            } else {
              StreamingService.save({}, {
                project: $scope.projectName,
                tableData:angular.toJson($scope.tableData),
                streamingConfig: angular.toJson($scope.streamingMeta),
                kafkaConfig: angular.toJson($scope.kafkaMeta)
              }, function (request) {
                if (request.successful) {
                  SweetAlert.swal('', 'Created the streaming successfully.', 'success');
                  $scope.cancel();
                  scope.aceSrcTbLoaded(true);
                } else {
                  var message = request.message;
                  var msg = !!(message) ? message : 'Failed to take action.';
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta,true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta,true)
                  }), 'error', {}, true, 'top_center');
                }
                loadingRequest.hide();
              }, function (e) {
                if (e.data && e.data.exception) {
                  var message = e.data.exception;
                  var msg = !!(message) ? message : 'Failed to take action.';

                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema':angular.toJson( $scope.streamingMeta,true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta,true)
                  }), 'error', {}, true, 'top_center');
                } else {
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta,true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta,true)
                  }), 'error', {}, true, 'top_center');
                }
                //end loading
                loadingRequest.hide();
              })
            }

          }
        });
      }

    };

  });

