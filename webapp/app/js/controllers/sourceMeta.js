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
  .controller('SourceMetaCtrl', function ($scope, $cacheFactory, $q, $window, $routeParams, CubeService, $modal, TableService, $route, loadingRequest, SweetAlert, tableConfig, TableModel, cubeConfig, MessageBox) {
    var $httpDefaultCache = $cacheFactory.get('$http');
    $scope.tableModel = TableModel;
    $scope.tableModel.selectedSrcDb = [];
    $scope.tableModel.selectedSrcTable = {};
    $scope.window = 0.68 * $window.innerHeight;
    $scope.tableConfig = tableConfig;
    $scope.isCalculate = true;
    $scope.selectedTsPattern = '';
    $scope.selfDefinedTsPattern = false;

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
      } else if (obj.datatype) {
        $scope.tableModel.selectedSrcTable.selectedSrcColumn = obj;
      }
    };

    $scope.aceSrcTbChanged = function () {
      $scope.tableModel.selectedSrcDb = [];
      $scope.tableModel.selectedSrcTable = {};
      $scope.aceSrcTbLoaded(true);
    };


    $scope.openModal = function () {
      if (!$scope.projectModel.selectedProject) {
        SweetAlert.swal('Oops...', "Please select a project.", 'info');
        return;
      }
      $modal.open({
        templateUrl: 'addHiveTable.html',
        controller: ModalInstanceCtrl,
        backdrop: 'static',
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

    $scope.openReloadModal = function () {
      $modal.open({
        templateUrl: 'reloadTable.html',
        controller: ModalInstanceCtrl,
        backdrop: 'static',
        resolve: {
          tableNames: function () {
            return $scope.tableModel.selectedSrcTable.database + '.' + $scope.tableModel.selectedSrcTable.name;
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
      if (!$scope.projectModel.selectedProject) {
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

    $scope.reloadTable = function (tableName, isCalculate) {
      var delay = $q.defer();
      loadingRequest.show();
      TableService.loadHiveTable({
        tableName: tableName,
        action: $scope.projectModel.selectedProject
      }, {calculate: isCalculate}, function (result) {
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
          MessageBox.successNotify('The following table(s) have been successfully loaded: ' + loadTableInfo);
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
          TableService.unLoadHiveTable({
            tableName: tableName,
            action: $scope.projectModel.selectedProject
          }, {}, function (result) {
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
              MessageBox.successNotify('The following table(s) have been successfully unloaded: ' + removedTableInfo);
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
      $scope.selectTable = tableNames;
      $scope.projectName = projectName;
      $scope.isCalculate = {
        val: true
      }

      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };

      $scope.kylinConfig = kylinConfig;


      $scope.treeOptions = {multiSelection: true};
      $scope.selectedNodes = [];
      $scope.hiveLimit = kylinConfig.getHiveLimit();
      $scope.sourceType = kylinConfig.getSourceType();
      if ($scope.sourceType !== '0') {
        $scope.isCalculate.val = false
      }

      $scope.loadHive = function () {
        if ($scope.hiveLoaded)
          return;
        TableService.showHiveDatabases({project: $scope.projectName}, function (databases) {
          $scope.dbNum = databases.length;
          if (databases.length > 0) {
            $scope.hiveMap = {};
            for (var i = 0; i < databases.length; i++) {
              var dbName = databases[i];
              var hiveData = {"dbname": dbName, "tables": [], "expanded": false};
              $scope.hive.push(hiveData);
              $scope.hiveMap[dbName] = i;
            }
          }
          $scope.hiveLoaded = true;
          $scope.showMoreDatabases();
        }, function (e) {
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Failed to take action.';
            SweetAlert.swal('Oops...', msg, 'error');
          } else {
            SweetAlert.swal('Oops...', "Failed to take action.", 'error');
          }
          $scope.hiveLoaded = true;
        });
      }

      $scope.showMoreTables = function (hiveTables, node) {
        var shownTimes = parseInt(node.children.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = 0;
        var hasMore = false;
        if (from + $scope.hiveLimit > hiveTables.length) {
          to = hiveTables.length - 1;
        } else {
          to = from + $scope.hiveLimit - 1;
          hasMore = true;
        }
        if (!angular.isUndefined(node.children[from])) {
          node.children.pop();
        }

        for (var idx = from; idx <= to; idx++) {
          node.children.push({"label": node.label + '.' + hiveTables[idx], "id": idx - from + 1, "children": []});
        }

        if (hasMore) {
          var loading = {"label": "", "id": 65535, "children": []};
          node.children.push(loading);
        }
      }

      $scope.showAllTables = function (hiveTables, node) {
        var shownTimes = parseInt(node.children.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = hiveTables.length - 1;
        if (!angular.isUndefined(node.children[from])) {
          node.children.pop();
        }
        for (var idx = from; idx <= to; idx++) {
          node.children.push({"label": node.label + '.' + hiveTables[idx], "id": idx - from + 1, "children": []});
        }
      }

      $scope.showMoreDatabases = function () {
        var shownTimes = parseInt($scope.treedata.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = 0;
        var hasMore = false;
        if (from + $scope.hiveLimit > $scope.hive.length) {
          to = $scope.hive.length - 1;
        } else {
          to = from + $scope.hiveLimit - 1;
          hasMore = true;
        }
        if (!angular.isUndefined($scope.treedata[from])) {
          $scope.treedata.pop();
        }

        for (var idx = from; idx <= to; idx++) {
          var children = [];
          var loading = {"label": "", "id": 0, "children": []};
          children.push(loading);
          $scope.treedata.push({
            "label": $scope.hive[idx].dbname,
            "id": idx + 1,
            "children": children,
            "expanded": false
          });
        }

        if (hasMore) {
          var loading = {"label": "", "id": 65535, "children": [0]};
          $scope.treedata.push(loading);
        }
      }

      $scope.showAllDatabases = function () {
        var shownTimes = parseInt($scope.treedata.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = $scope.hive.length - 1;

        if (!angular.isUndefined($scope.treedata[from])) {
          $scope.treedata.pop();
        }

        for (var idx = from; idx <= to; idx++) {
          var children = [];
          var loading = {"label": "", "id": 0, "children": []};
          children.push(loading);
          $scope.treedata.push({
            "label": $scope.hive[idx].dbname,
            "id": idx + 1,
            "children": children,
            "expanded": false
          });
        }
      }

      $scope.showMoreClicked = function ($parentNode) {
        if ($parentNode == null) {
          $scope.showMoreDatabases();
        } else {
          $scope.showMoreTables($scope.hive[$scope.hiveMap[$parentNode.label]].tables, $parentNode);
        }
      }

      $scope.showAllClicked = function ($parentNode) {
        if ($parentNode == null) {
          $scope.showAllDatabases();
        } else {
          $scope.showAllTables($scope.hive[$scope.hiveMap[$parentNode.label]].tables, $parentNode);
        }
      }

      $scope.showToggle = function (node) {
        if (node.expanded == false) {
          TableService.showHiveTables({"database": node.label, project: $scope.projectName}, function (hive_tables) {
            var tables = [];
            for (var i = 0; i < hive_tables.length; i++) {
              tables.push(hive_tables[i]);
            }
            $scope.hive[$scope.hiveMap[node.label]].tables = tables;
            $scope.showMoreTables(tables, node);
            node.expanded = true;
          });
        }
      }

      $scope.showSelected = function (node) {

      }

      if (angular.isUndefined($scope.hive) || angular.isUndefined($scope.hiveLoaded) || angular.isUndefined($scope.treedata)) {
        $scope.hive = [];
        $scope.hiveLoaded = false;
        $scope.treedata = [];
        $scope.loadHive();
      }

      $scope.confirmReload = function() {
        $scope.cancel();
        scope.reloadTable($scope.selectTable, $scope.isCalculate.val).then(function() {
          scope.aceSrcTbLoaded(true);
        })
      }


      $scope.add = function () {

        if ($scope.tableNames.length === 0 && $scope.selectedNodes.length > 0) {
          for (var i = 0; i < $scope.selectedNodes.length; i++) {
            if ($scope.selectedNodes[i].label.indexOf(".") >= 0) {
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
        scope.reloadTable($scope.tableNames, $scope.isCalculate.val).then(function () {
          scope.aceSrcTbLoaded(true);
        });
      }


    };

    $scope.editStreamingConfig = function (tableName) {
      var modalInstance = $modal.open({
        templateUrl: 'editStreamingSource.html',
        controller: EditStreamingSourceCtrl,
        backdrop: 'static',
        resolve: {
          tableNames: function () {
            return $scope.tableNames;
          },
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          tableName: function () {
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

    $scope.editStreamingConfigV2 = function (streamingConfig) {
      var modalInstance = $modal.open({
        templateUrl: 'editStreamingTableV2.html',
        controller: EditStreamingSourceV2Ctrl,
        backdrop: 'static',
        resolve: {
          streamingConfig: function () {
            return streamingConfig;
          },
          projectName: function () {
            return $scope.projectModel.selectedProject;
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
      if (!$scope.projectModel.selectedProject) {
        SweetAlert.swal('Oops...', "Please select a project.", 'info');
        return;
      }
      $modal.open({
        templateUrl: 'addStreamingSource.html',
        controller: StreamingSourceCtrl,
        backdrop: 'static',
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

    function bootstrapServerValidation(bootstrapServers) {
      var flag = false;
      if (bootstrapServers && bootstrapServers.length > 0) {
        angular.forEach(bootstrapServers, function (bootstrapServer, ind) {
          if (!bootstrapServer.host || !bootstrapServer.port || bootstrapServer.host.length === 0 || bootstrapServer.port.length === 0) {
            flag = true;
          }
        });
      } else {
        flag = true;
      }
      return flag;
    };
    var EditStreamingSourceCtrl = function ($scope, $interpolate, $templateCache, tableName, $modalInstance, tableNames, MessageService, projectName, scope, tableConfig, cubeConfig, StreamingModel, StreamingService) {

      $scope.state = {
        tableName: tableName,
        mode: "edit",
        target: "kfkConfig"
      }

      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };

      $scope.projectName = projectName;
      $scope.streamingMeta = StreamingModel.createStreamingConfig();
      $scope.kafkaMeta = StreamingModel.createKafkaConfig();
      $scope.updateStreamingMeta = function (val) {
        $scope.streamingMeta = val;
      }
      $scope.updateKafkaMeta = function (val) {
        $scope.kafkaMeta = val;
      }

      $scope.updateStreamingSchema = function () {
        StreamingService.update({}, {
          project: $scope.projectName,
          tableData: angular.toJson(""),
          streamingConfig: angular.toJson($scope.streamingMeta),
          kafkaConfig: angular.toJson($scope.kafkaMeta)
        }, function (request) {
          if (request.successful) {
            MessageBox.successNotify('Updated the streaming successfully.');
            $scope.cancel();
          } else {
            var message = request.message;
            var msg = !!(message) ? message : 'Failed to take action.';
            MessageService.sendMsg($scope.streamingResultTmpl({
              'text': msg,
              'streamingSchema': angular.toJson($scope.streamingMeta, true),
              'kfkSchema': angular.toJson($scope.kafkaMeta, true)
            }), 'error', {}, true, 'top_center');
          }
          loadingRequest.hide();
        }, function (e) {
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Failed to take action.';
            MessageService.sendMsg($scope.streamingResultTmpl({
              'text': msg,
              'streamingSchema': angular.toJson($scope.streamingMeta, true),
              'kfkSchema': angular.toJson($scope.kafkaMeta, true)
            }), 'error', {}, true, 'top_center');
          } else {
            MessageService.sendMsg($scope.streamingResultTmpl({
              'text': msg,
              'streamingSchema': angular.toJson($scope.streamingMeta, true),
              'kfkSchema': angular.toJson($scope.kafkaMeta, true)
            }), 'error', {}, true, 'top_center');
          }
          //end loading
          loadingRequest.hide();

        })
      }

    }

    var EditStreamingSourceV2Ctrl = function ($scope, ResponseUtil, $modalInstance, projectName, StreamingServiceV2, streamingConfig) {
      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };
      $scope.streamingConfig = streamingConfig;
      $scope.streamingConfig.properties.bootstrapServers = streamingConfig.properties['bootstrap.servers'].split(',').map(function (address) {
        return {
          host: address.split(':')[0],
          port: +address.split(':')[1]
        }
      })
      $scope.addBootstrapServer = function () {
        if (!$scope.streamingConfig.properties.bootstrapServers) {
          $scope.streamingConfig.properties.bootstrapServers = [];
        }
        $scope.streamingConfig.properties.bootstrapServers.push({host: '', port: 9092});
      }
      $scope.removeBootstrapServer = function (index) {
        $scope.streamingConfig.properties.bootstrapServers.splice(index, 1);
      };
      $scope.projectName = projectName;
      $scope.updateStreamingMeta = function (val) {
        $scope.streamingMeta = val;
      }
      $scope.updateKafkaMeta = function (val) {
        $scope.kafkaMeta = val;
      }
      $scope.bootstrapServerValidation = bootstrapServerValidation
      $scope.updateStreamingV2Config = function () {
        loadingRequest.show();
        $scope.streamingConfig.properties['bootstrap.servers'] = $scope.streamingConfig.properties.bootstrapServers.map(function (address) {
          return address.host + ':' + address.port;
        }).join(',');
        delete $scope.streamingConfig.properties.bootstrapServers;
        var updateConfig = {
          project: $scope.projectName,
          streamingConfig: JSON.stringify($scope.streamingConfig)
        }
        StreamingServiceV2.update({}, updateConfig, function (request) {
          if (request.successful) {
            MessageBox.successNotify('Updated the streaming successfully.');
            $scope.cancel();
          } else {
            ResponseUtil.handleError({
              data: {exception: request.message}
            })
          }
          loadingRequest.hide();
        }, function (e) {
          ResponseUtil.handleError(e)
          loadingRequest.hide();
        })
      }
    }

    // 推断列的类型
    function checkColumnValType(val, key) {
      var defaultType;
      if (typeof val === "number") {
        if (/id/i.test(key) && val.toString().indexOf(".") == -1) {
          defaultType = "int";
        } else if (val <= 2147483647) {
          if (val.toString().indexOf(".") != -1) {
            defaultType = "decimal";
          } else {
            defaultType = "int";
          }
        } else {
          defaultType = "timestamp";
        }
      } else if (typeof val === "string") {
        if (!isNaN((new Date(val)).getFullYear()) && typeof ((new Date(val)).getFullYear()) === "number") {
          defaultType = "date";
        } else {
          defaultType = "varchar(256)";
        }
      } else if (Object.prototype.toString.call(val) == "[object Array]") {
        defaultType = "varchar(256)";
      } else if (typeof val === "boolean") {
        defaultType = "boolean";
      }
      return defaultType;
    }

    // 打平straming表结构
    function flatStreamingJson(objRebuildFunc, flatResult) {
      return function flatObj(obj, base, comment) {
        base = base ? base + "_" : "";
        comment = comment ? comment + "|" : ""
        for (var i in obj) {
          if (Object.prototype.toString.call(obj[i]) == "[object Object]") {
            flatObj(obj[i], base + i, comment + i);
            continue;
          }
          flatResult.push(objRebuildFunc(base + i, obj[i], comment + i));
        }
      }
    }

    var StreamingSourceCtrl = function ($scope, $location, $interpolate, $templateCache, $modalInstance, tableNames, MessageService, projectName, scope, tableConfig, cubeConfig, StreamingModel, StreamingService) {

      $scope.state = {
        'mode': 'edit'
      }

      $scope.streamingMeta = StreamingModel.createStreamingConfig();
      $scope.kafkaMeta = StreamingModel.createKafkaConfig();


      $scope.steps = {
        curStep: 1
      };

      $scope.streamingCfg = {
        parseTsColumn: "{{}}",
        columnOptions: []
      }

      $scope.previewStep = function () {
        $scope.steps.curStep--;
      }

      $scope.nextStep = function () {

        $scope.checkFailed = false;

        //check form
        $scope.form['setStreamingSchema'].$submitted = true;
        if (!$scope.streaming.sourceSchema || $scope.streaming.sourceSchema === "") {
          $scope.checkFailed = true;
        }

        if (!$scope.table.name || $scope.table.name === "") {
          $scope.checkFailed = true;
        }

        $scope.prepareNextStep();

        if (!$scope.rule.timestampColumnExist) {
          $scope.checkFailed = true;
        }

        if ($scope.checkFailed) {
          return;
        }

        $scope.steps.curStep++;
      }

      $scope.prepareNextStep = function () {
        $scope.streamingCfg.columnOptions = [];
        $scope.rule.timestampColumnExist = false;
        angular.forEach($scope.columnList, function (column, $index) {
          if (column.checked == "Y" && column.fromSource == "Y" && column.type == "timestamp") {
            $scope.streamingCfg.columnOptions.push(column.name);
            $scope.rule.timestampColumnExist = true;
          }
        })

        if ($scope.streamingCfg.columnOptions.length == 1) {
          $scope.streamingCfg.parseTsColumn = $scope.streamingCfg.columnOptions[0];
          $scope.kafkaMeta.parserProperties = "tsColName=" + $scope.streamingCfg.parseTsColumn;
        }
        if ($scope.kafkaMeta.parserProperties !== '') {
          $scope.state.isParserHeaderOpen = false;
        } else {
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
        sourceValid: false,
        schemaChecked: false
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
        try {
          $scope.streaming.parseResult = JSON.parse($scope.streaming.sourceSchema);
        } catch (error) {
          $scope.table.sourceValid = false;
          return;
        }
        $scope.table.sourceValid = true;

        //streaming table data change structure
        function createNewObj(key, val, comment) {
          var obj = {};
          obj.name = key;
          obj.type = checkColumnValType(val, key);
          obj.fromSource = "Y";
          obj.checked = "Y";
          obj.comment = comment;
          if (Object.prototype.toString.call(val) == "[object Array]") {
            obj.checked = "N";
          }
          return obj;
        }

        var columnList = []
        flatStreamingJson(createNewObj, columnList)($scope.streaming.parseResult)
        var timeMeasure = $scope.cubeConfig.streamingAutoGenerateMeasure;
        for (var i = 0; i < timeMeasure.length; i++) {
          var defaultCheck = 'Y';
          columnList.push({
            'name': timeMeasure[i].name,
            'checked': defaultCheck,
            'type': timeMeasure[i].type,
            'fromSource': 'N'
          });
        }

        var firstCommit = false;
        if ($scope.columnList.length == 0) {
          firstCommit = true;
        }

        if (!firstCommit) {
          angular.forEach(columnList, function (item) {
            for (var i = 0; i < $scope.columnList.length; i++) {
              if ($scope.columnList[i].name == item.name) {
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


      $scope.form = {};
      $scope.rule = {
        'timestampColumnExist': false
      }

      $scope.modelMode == "addStreaming";

      $scope.syncStreamingSchema = function () {

        $scope.form['cube_streaming_form'].$submitted = true;

        if ($scope.form['cube_streaming_form'].parserName.$invalid || $scope.form['cube_streaming_form'].parserProperties.$invalid) {
          $scope.state.isParserHeaderOpen = true;
        }

        if ($scope.form['cube_streaming_form'].$invalid) {
          return;
        }

        var columns = [];
        angular.forEach($scope.columnList, function (column, $index) {
          if (column.checked == "Y") {
            var columnInstance = {
              "id": ++$index,
              "name": column.name,
              "comment": /[|]/.test(column.comment) ? column.comment : "",
              "datatype": column.type
            }
            columns.push(columnInstance);
          }
        })


        $scope.tableData = {
          "name": $scope.table.name,
          "source_type": 1,
          "columns": columns,
          'database': 'Default'
        }


        $scope.kafkaMeta.name = $scope.table.name
        $scope.streamingMeta.name = $scope.table.name;

        SweetAlert.swal({
          title: "",
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
                tableData: angular.toJson($scope.tableData),
                streamingConfig: angular.toJson($scope.streamingMeta),
                kafkaConfig: angular.toJson($scope.kafkaMeta)
              }, function (request) {
                if (request.successful) {
                  MessageBox.successNotify('Updated the streaming successfully.');
                  $location.path("/models");
                } else {
                  var message = request.message;
                  var msg = !!(message) ? message : 'Failed to take action.';
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta, true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta, true)
                  }), 'error', {}, true, 'top_center');
                }
                loadingRequest.hide();
              }, function (e) {
                if (e.data && e.data.exception) {
                  var message = e.data.exception;
                  var msg = !!(message) ? message : 'Failed to take action.';
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta, true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta, true)
                  }), 'error', {}, true, 'top_center');
                } else {
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta, true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta, true)
                  }), 'error', {}, true, 'top_center');
                }
                //end loading
                loadingRequest.hide();

              })
            } else {
              StreamingService.save({}, {
                project: $scope.projectName,
                tableData: angular.toJson($scope.tableData),
                streamingConfig: angular.toJson($scope.streamingMeta),
                kafkaConfig: angular.toJson($scope.kafkaMeta)
              }, function (request) {
                if (request.successful) {
                  MessageBox.successNotify('Created the streaming successfully.');
                  $scope.cancel();
                  scope.aceSrcTbLoaded(true);
                } else {
                  var message = request.message;
                  var msg = !!(message) ? message : 'Failed to take action.';
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta, true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta, true)
                  }), 'error', {}, true, 'top_center');
                }
                loadingRequest.hide();
              }, function (e) {
                if (e.data && e.data.exception) {
                  var message = e.data.exception;
                  var msg = !!(message) ? message : 'Failed to take action.';

                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta, true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta, true)
                  }), 'error', {}, true, 'top_center');
                } else {
                  MessageService.sendMsg($scope.streamingResultTmpl({
                    'text': msg,
                    'streamingSchema': angular.toJson($scope.streamingMeta, true),
                    'kfkSchema': angular.toJson($scope.kafkaMeta, true)
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

    //streaming resource onboard v2
    $scope.openStreamingSourceModalV2 = function () {
      if (!$scope.projectModel.selectedProject) {
        SweetAlert.swal('Oops...', "Please select a project.", 'info');
        return;
      }
      $modal.open({
        templateUrl: 'addStreamingSourceV2.html',
        controller: StreamingSourceCtrlV2,
        backdrop: 'static',
        resolve: {
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          scope: function () {
            return $scope;
          }
        }
      });
    };

    var StreamingSourceCtrlV2 = function ($scope, $modalInstance, $filter, MessageService, projectName, scope, cubeConfig, tableConfig, StreamingModel, StreamingServiceV2) {
      $scope.tableConfig = tableConfig;
      // common
      $scope.steps = {
        curStep: 1
      };

      $scope.previewStep = function () {
        $scope.steps.curStep--;
      }

      $scope.nextStep = function () {
        // clean data
        if ($scope.steps.curStep === 1) {
          $scope.streaming = {
            template: '',
            dataTypeArr: tableConfig.dataTypes,
            TSColumnArr: [],
            TSPatternArr: ['MS', 'S'],
            TSParserArr: ['org.apache.kylin.stream.source.kafka.LongTimeParser', 'org.apache.kylin.stream.source.kafka.DateTimeParser'],
            TSColumnSelected: '',
            TSParser: 'org.apache.kylin.stream.source.kafka.LongTimeParser',
            TSPattern: 'MS',
            errMsg: ''
          };
          $scope.tableData = {
            source_type: $scope.tableData.source_type
          };
        }
        $scope.steps.curStep++;
      };

      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };

      // streaming config
      $scope.streamingConfig = {
        name: '',
        properties: {},
        parser_info: {}
      };

      $scope.tableData = {
        source_type: tableConfig.streamingSourceType.kafka
      };

      $scope.removeBootstrapServer = function (index) {
        $scope.streamingConfig.properties.bootstrapServers.splice(index, 1);
      };

      $scope.addBootstrapServer = function () {
        if (!$scope.streamingConfig.properties.bootstrapServers) {
          $scope.streamingConfig.properties.bootstrapServers = [];
        }
        $scope.streamingConfig.properties.bootstrapServers.push({host: '', port: '9092'});
      }

      $scope.bootstrapServerValidation = bootstrapServerValidation;

      // streaming table
      $scope.streaming = {
        template: '',
        dataTypeArr: tableConfig.dataTypes,
        TSColumnArr: [],
        TSColumnSelected: '',
        TSParser: '',
        TSPattern: '',
        errMsg: '',
        lambda: false
      };

      $scope.additionalColumn = {};

      $scope.getTemplate = function () {
        var transformStreamingConfig = $scope._transformStreamingObj({
          project: projectName,
          tableData: $scope.tableData,
          streamingConfig: $scope.streamingConfig
        });
        StreamingServiceV2.getParserTemplate({
          streamingConfig: transformStreamingConfig.streamingConfig,
          sourceType: $scope.tableData.source_type
        }, function (template) {
          if (tableConfig.streamingSourceType.kafka === $scope.tableData.source_type) {
            $scope.streaming.template = $filter('json')(template, 4);
          }
        }, function (e) {
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Failed to get template.';
            SweetAlert.swal('Oops...', msg, 'error');
          } else {
            SweetAlert.swal('Oops...', "Failed to get template.", 'error');
          }
        });
      };

      $scope.getTableData = function () {
        $scope.tableData.name = '';
        $scope.tableData.columns = [];

        $scope.streaming.errMsg = '';

        $scope.streaming.TSColumnArr = [];

        // Check template is not empty
        if (!$scope.streaming.template) {
          // $scope.tableData = undefined;
          $scope.tableData = {
            source_type: $scope.tableData.source_type
          };
          $scope.streaming.errMsg = 'Please input Streaming source record to generate schema.';
          return;
        }

        // Check template is json format
        try {
          var templateObj = JSON.parse($scope.streaming.template);
        } catch (error) {
          $scope.tableData = {
            source_type: $scope.tableData.source_type
          };
          $scope.streaming.errMsg = 'Source json invalid, Please correct your schema and generate again.';
          return;
        }
        // kafka parser
        var columnsByTemplate = []

        function createNewObj(key, val, comment) {
          var obj = {};
          obj.name = key;
          obj.datatype = checkColumnValType(val, key);
          obj.comment = comment;
          if (obj.datatype === 'timestamp') {
            $scope.streaming.TSColumnArr.push(key);
          }
          return obj;
        }

        if (tableConfig.streamingSourceType.kafka === $scope.tableData.source_type) {
          // TODO kafka need to support json not just first layer
          flatStreamingJson(createNewObj, columnsByTemplate)(templateObj)
        }
        var columnsByAuto = [];

        // TODO change the streamingAutoGenerateMeasure format
        angular.forEach(cubeConfig.streamingAutoGenerateMeasure, function (measure, index) {
          columnsByAuto.push({
            name: measure.name,
            datatype: measure.type
          });
        });

        $scope.tableData.columns = columnsByTemplate.concat(columnsByAuto);
      };

      $scope.getColumns = function (field, parentName, columnArr) {
        var columns = columnArr;
        if (typeof field.type === 'string') {
          if (!field.fields) {
            columns.push($scope.getColumnInfo(field, ''));
          } else {
            angular.forEach(field.fields, function (subField, ind) {
              var subColumn = $scope.getColumnInfo(subField, parentName);
              if (!$scope.isDerived(subColumn)) {
                columns.push(subColumn);
              }
            });
          }
        } else if (Array.isArray(field.type)) {
          columns.push($scope.getColumnInfo(field, ''));
        } else {
          $scope.getColumns(field.type, parentName, columns);
        }
        return columns;
      };

      $scope.getColumnInfo = function (field, parentName) {
        var fieldName = field.name;
        var contentType = 'varchar(256)';
        var typeArr = field.type;
        var _type = field.type;
        if (typeof typeArr !== 'string') {
          var contentTypeArr = [];
          angular.forEach(typeArr, function (type) {
            if ('null' !== type) {
              contentTypeArr.push(type);
            }
          });
          if (contentTypeArr.length === 1) {
            _type = contentTypeArr[0];
            if (typeof _type === 'object') {
              if (_type.type) {
                _type = _type.type;
              }
            }
          }
        }

        if ('string' !== _type) {
          if (tableConfig.dataTypes.indexOf(_type) > -1) {
            contentType = _type;
          } else if ('long' === _type) {
            contentType = 'bigint';
          }
        }

        var _column = {
          name: fieldName,
          datatype: contentType,
          comment: field.doc || '',
          field_mapping: parentName ? parentName + '.' + field.name : field.name
        };

        return _column;
      };

      $scope.removeColumn = function (index) {
        $scope.tableData.columns.splice(index, 1);
      };

      $scope.addColumn = function () {
        if ($scope.additionalColumn.name && $scope.additionalColumn.datatype && $scope.additionalColumn.field_mapping) {
          $scope.additionalColumn.error = undefined;
          if (!$scope.tableData.columns) {
            $scope.tableData.columns = [];
          }
          $scope.tableData.columns.push($scope.additionalColumn);
          if ($scope.additionalColumn.datatype == 'timestamp') {
            if (!$scope.streaming.TSColumnArr) {
              $scope.streaming.TSColumnArr = [];
            }
            $scope.streaming.TSColumnArr.push($scope.additionalColumn.name);
          }
          $scope.additionalColumn = {};
        } else {
          $scope.additionalColumn.error = 'Additional column field can not be empty!';
        }
      };

      $scope.initFieldMapping = function () {
        $scope.additionalColumn.field_mapping = $scope.additionalColumn.name;
      };

      $scope.isDerived = function (column) {
        var derived = false;
        angular.forEach(cubeConfig.streamingAutoGenerateMeasure, function (measure, index) {
          if (measure.name === column.name) {
            derived = true;
          }
        });
        return derived;
      };

      $scope.updateTSColumnOption = function (column) {
        if (column.datatype === 'timestamp') {
          if (!$scope.streaming.TSColumnArr.includes(column.name)) {
            $scope.streaming.TSColumnArr.push(column.name);
          }
        } else {
          if ($scope.streaming.TSColumnArr.includes(column.name)) {
            $scope.streaming.TSColumnArr.splice($scope.streaming.TSColumnArr.findIndex(function (opt) {
              return opt === column.name
            }), 1);
            if (column.name === $scope.streaming.TSColumnSelected) {
              $scope.streaming.TSColumnSelected = '';
            }
          }
        }
      };

      $scope.updateDateTimeParserOption = function (parser) {
        if (parser === $scope.streaming.TSParserArr[0]) {
          $scope.streaming.TSPatternArr = [];
          $scope.streaming.TSPatternArr.push('MS');
          $scope.streaming.TSPatternArr.push('S');
          $scope.streaming.TSPattern = 'MS';
        } else if (parser === $scope.streaming.TSParserArr[1]) {
          $scope.streaming.TSPatternArr = [];
          TableService.getSupportedDatetimePatterns({}, function (patterns) {
            $scope.streaming.TSPatternArr = patterns;
            $scope.streaming.TSPatternArr.push('--- Other ---');
            $scope.streaming.TSPattern = 'yyyy-MM-dd HH:mm:ss.SSS';
          }, function (e) {
            return;
          });
        }
      };

      $scope.updateTsPatternOption = function (pattern) {
        if (pattern === '--- Other ---') {
          $scope.selfDefinedTsPattern = true;
          $scope.streaming.TSPattern = '';
        } else {
          $scope.selfDefinedTsPattern = pattern;
          $scope.selfDefinedTsPattern = false;
        }
      };

      $scope.saveStreamingSource = function () {
        $scope.streaming.errMsg = '';

        if (!$scope.validateTableName()) {
          $scope.streaming.errMsg = 'Table name is invalid, please typing correct table name.';
          return;
        }

        // table column validation
        if ($scope.tableData.columns.length === 0) {
          $scope.streaming.errMsg = 'Table columns is empty, please add template to create it.';
          return;
        }

        var allColumnsAreDerived = true;
        angular.forEach($scope.tableData.columns, function (column) {
          if (!$scope.isDerived(column)) {
            allColumnsAreDerived = false;
          }
        });

        if (allColumnsAreDerived) {
          $scope.streaming.errMsg = 'All columns are derived, please add template to create it again.';
          return;
        }


        var streamingSourceConfigStr = '';

        // kafka config validation
        if (!$scope.streaming.TSColumnSelected) {
          $scope.streaming.errMsg = 'TSColumn is empty, please choose \'timestamp\' type column for TSColumn.';
          return;
        }
        // Set ts column
        $scope.streamingConfig.parser_info.ts_col_name = $scope.streaming.TSColumnSelected;
        $scope.streamingConfig.parser_info.ts_parser = $scope.streaming.TSParser;
        $scope.streamingConfig.parser_info.ts_pattern = $scope.streaming.TSPattern;
        $scope.streamingConfig.parser_info.field_mapping = {};
        $scope.tableData.columns.forEach(function (col) {
          if (col.comment) {
            $scope.streamingConfig.parser_info.field_mapping[col.name] = col.comment.replace(/\|/g, '.') || ''
          }
        })
        SweetAlert.swal({
          title: '',
          text: 'Are you sure to save the streaming table?',
          showCancelButton: true,
          confirmButtonColor: '#DD6B55',
          confirmButtonText: "Yes",
          closeOnConfirm: true
        }, function (isConfirm) {
          if (isConfirm) {

            $scope.streamingConfig.name = $scope.tableData.name;

            // add column id
            var colInd = 0;
            angular.forEach($scope.tableData.columns, function (column) {
              column.id = colInd;
              colInd++;
            });
            var transformStreamingConfig = angular.toJson($scope._transformStreamingObj({
              project: projectName,
              tableData: $scope.tableData,
              streamingConfig: $scope.streamingConfig
            }));
            StreamingServiceV2.save({},
              transformStreamingConfig
              , function (request) {
                if (request.successful) {
                  SweetAlert.swal('', 'Created the streaming successfully.', 'success');
                  $scope.cancel();
                  scope.aceSrcTbLoaded(true);
                } else {
                  var message = request.message;
                  var msg = !!(message) ? message : 'Failed to create streaming source.';
                  $scope.streaming.errMsg = msg;
                }
                loadingRequest.hide();
              }, function (e) {
                if (e.data && e.data.exception) {
                  var message = e.data.exception;
                  var msg = !!(message) ? message : 'Failed to create streaming source.';
                  $scope.streaming.errMsg = msg;
                } else {
                  $scope.streaming.errMsg = 'Failed to create streaming source.';
                }
                //end loading
                loadingRequest.hide();
              });
          }
        });

      };

      $scope.validateTableName = function () {
        var tableName = $scope.tableData.name;
        if (tableName && tableName.length > 0) {
          if (tableName.split('.').length < 3 && tableName.indexOf('.') !== 0) {
            return true;
          } else {
            return false;
          }
        }
        return false;
      };

      $scope._transformStreamingObj = function (streamingObject) {
        var streamingRequest = {};
        streamingRequest.project = streamingObject.project;
        var streamingConfig = angular.copy(streamingObject.streamingConfig);
        streamingRequest.tableData = angular.copy(streamingObject.tableData);
        if (tableConfig.streamingSourceType.kafka === streamingRequest.tableData.source_type) {
          // Set bootstrap servers
          var bootstrapServersStr = '';
          angular.forEach(streamingObject.streamingConfig.properties.bootstrapServers, function (bootstrapServer, index) {
            bootstrapServersStr += ',' + bootstrapServer.host + ':' + bootstrapServer.port;
          });
          streamingConfig.properties['bootstrap.servers'] = bootstrapServersStr.substring(1);
          delete streamingConfig.properties.bootstrapServers;
        }
        if ($scope.streaming.lambda) {
          streamingRequest.tableData.source_type = streamingRequest.tableData.source_type + 1;
        }
        streamingRequest.tableData = angular.toJson(streamingRequest.tableData);
        streamingRequest.streamingConfig = angular.toJson(streamingConfig);
        return streamingRequest;
      };

    };

    $scope.loadCsvFile = function () {
      $modal.open({
        templateUrl: 'addCsvSource.html',
        controller: CsvSourceCtrl,
        backdrop: 'static',
        resolve: {
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          scope: function () {
            return $scope;
          }
        }
      });
    };

    var CsvSourceCtrl = function ($scope, $modalInstance, SweetAlert, CsvUploadService, tableConfig, projectName, scope) {
      $scope.file = '';
      $scope.separator_list = ['comma', 'space', 'tab'];
      $scope.tableConfig = tableConfig;
      $scope.tableData = {
        name: '',
        has_header: false,
        separator: 'comma',
        loaded: false
      };

      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };

      $scope.loadTable = function () {
        var file = $scope.file;
        CsvUploadService.upload(file, $scope.tableData.has_header, $scope.tableData.separator)
          .then(
            function (response) {
              $scope.tableData.loaded = true;
              $scope.columnList = JSON.parse(response);
            },
            function (errResponse) {
              SweetAlert.swal('',JSON.parse(errResponse.data).msg, 'error')
            }
          );
      }

      $scope.saveTable = function () {
        var file = $scope.file;
        var columns = [];
        for (var i = 0; i < $scope.columnList.length; i++) {
          columns.push(JSON.stringify($scope.columnList[i]));
        }
        CsvUploadService.save(file, $scope.tableData.name, projectName, JSON.stringify(columns), $scope.tableData.has_header, $scope.tableData.separator)
          .then(
            function (response) {
              SweetAlert.swal('', 'Created table from csv file successfully.', 'success');
              $scope.cancel();
              scope.aceSrcTbLoaded(true);
            },
            function (errResponse) {
              SweetAlert.swal('',JSON.parse(errResponse.data).msg, 'error')
            }
          );
      }
    };
  });

/*snapshot controller*/
KylinApp
  .controller('TableSnapshotCtrl', function ($scope, TableService, CubeService, uiGridConstants) {
    $scope.initSnapshots = function () {
      var tableFullName = $scope.tableModel.selectedSrcTable.database + '.' + $scope.tableModel.selectedSrcTable.name
      TableService.getSnapshots({
        tableName: tableFullName,
        pro: $scope.projectModel.selectedProject
      }, {}, function (data) {
        var orgData = JSON.parse(angular.toJson(data));
        angular.forEach(orgData, function (snapshot) {
          if (!!snapshot.cubesAndSegmentsUsage && snapshot.cubesAndSegmentsUsage.length > 0) {
            snapshot.usageInfo = '';
            angular.forEach(snapshot.cubesAndSegmentsUsage, function (info) {
              snapshot.usageInfo += info;
              snapshot.usageInfo += '</br>';
            });
          } else {
            snapshot.usageInfo = 'No Usage Info';
          }
        });
        $scope.tableSnapshots = orgData;
      });
    };
    $scope.$watch('tableModel.selectedSrcTable', function (newValue, oldValue) {
      if (!newValue || !newValue.name) {
        return;
      }
      $scope.initSnapshots();
    });
  });

/*Avoid watch method call twice*/
KylinApp
  .controller('StreamConfigDisplayCtrl', function ($scope, StreamingServiceV2, tableConfig) {
    $scope.$watch('tableModel.selectedSrcTable', function (newValue, oldValue) {
      if (!newValue) {
        return;
      }
      if (_.values(tableConfig.streamingSourceType).indexOf($scope.tableModel.selectedSrcTable.source_type) > -1) {
        var table = $scope.tableModel.selectedSrcTable;
        var streamingName = table.database + "." + table.name;
        StreamingServiceV2.getConfig({table: streamingName}, function (configs) {
          $scope.currentStreamingConfig = configs[0];
        });
      }
    });
  });
