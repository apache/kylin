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
    .controller('QueryCtrl', function ($scope, storage, $base64, $q, $location, $anchorScroll, $routeParams, QueryService, CubeService, $modal, MessageService, $domUtilityService, $timeout, TableService, SweetAlert, VdmUtil) {
        $scope.mainPanel = 'query';
        if ($routeParams.queryPanel) {
            $scope.mainPanel = $routeParams.queryPanel;
        }
        $scope.rowsPerPage = 50000;
        $scope.hasLimit = true;
        $scope.base64 = $base64;
        $scope.queryString = "";
        $scope.queries = [];
        $scope.curQuery = null;
        $scope.exportSql = null;
        $scope.dateTypes = [91, 92, 93];
        $scope.stringTypes = [-1, 1, 12];
        $scope.numberTypes = [-7, -6, -5, 3, 4, 5, 6, 7, 8];
        $scope.ui = {fullScreen: false};
        $scope.chartTypes = [
            {name: "Line Chart", value: "line", dimension: { types: ['date'], multiple: false }, metrics: { multiple: false }},
            {name: "Bar Chart", value: "bar", dimension: { types: ['date', 'string'], multiple: false }, metrics: { multiple: false }},
            {name: "Pie Chart", value: "pie", dimension: { types: ['date', 'string'], multiple: false }, metrics: { multiple: false }}
        ];
        $scope.statusList = [
            {name: 'All', value: ''},
            {name: 'Executing', value: 'executing'},
            {name: 'Success', value: 'success'},
            {name: 'Failed', value: 'failed'}
        ];
        $scope.statusFilter = null;
        $scope.savedQueries = null;
        $scope.srcTables = [];
        $scope.srcColumns = [];

        $scope.curProject = null;
        $scope.state = {
            selectedProject: null
        };

        $scope.locationChangeConfirmed = false;
        $scope.specifyCube = null;
        $scope.cubeItems = [];

        var Query = {
            createNew: function (sql, project) {
                var query = {
                    originSql: sql,
                    sql: sql,
                    project: (!!project)? project:$scope.projectModel.getSelectedProject(),
                    status: 'executing',
                    acceptPartial: true,
                    result: {
                        isResponsePartial: false
                    },
                    graph: {
                        meta: {
                            dimensions: [], //Keys metadata
                            metrics: [] //Values metadata
                        },
                        state: {
                            dimensions: [], // User selected dimensions
                            metrics: []  // User selected metrics
                        },
                        type: {}, //Graph type
                        show: false
                    },
                    startTime: new Date()
                };

                return query;
            },

            resetQuery: function (query) {
                query.status = 'executing';
                query.result = {
                    isResponsePartial: false
                };
                query.acceptPartial = true;
                query.graph = {
                    meta: {
                        dimensions: [], //Keys metadata
                        metrics: [] //Values metadata
                    },
                    state: {
                        dimensions: [], // User selected dimensions
                        metrics: []  // User selected metrics
                    },
                    type: {}, //Graph type
                    show: false
                };
                query.startTime = new Date();
            }
        }

        $scope.checkLimit = function () {
          if (!$scope.rowsPerPage) {
            $scope.rowsPerPage = 50000;
          }
        }

        $scope.changeLimit = function () {
            if ($scope.hasLimit) {
              $scope.rowsPerPage = 50000;
            } else {
              $scope.rowsPerPage = 0
            }
        }

        function getQuery(queries, query) {
            for (var i = 0; i < queries.length; i++) {
                if (queries[i].sql === query.sql && queries[i].project === query.project) {
                    return queries[i];
                }
            }

            return null;
        }

        function makeSuffixRegExp(suffix, caseInsensitive) {
            return new RegExp(
                    String(suffix).replace(/[$%()*+.?\[\\\]{|}]/g, "\\$&") + "$",
                caseInsensitive ? "i" : "");
        }

        function justShowTable() {
            var words = $scope.queryString.split(/\s+/);
            words.splice(words.length - 1, words.length);

            return makeSuffixRegExp("from", true).test(words.toString()) || makeSuffixRegExp("join", true).test(words.toString());
        }

        $scope.setQueryString = function (queryString) {
            $scope.queryString = queryString;
        }

        $scope.aceLoaded = function (_editor) {
            var langTools = ace.require("ace/ext/language_tools");

            if (!langTools.tableAdded) {
                langTools.addCompleter({
                    getCompletions: function (editor, session, pos, prefix, callback) {
                        if (prefix.length === 0) {
                            callback(null, []);
                        } else {
                            callback(null, $scope.srcTables);
                        }
                    }
                });
                langTools.tableAdded = true;
            }

            if (!langTools.columnAdded) {
                langTools.addCompleter({
                    getCompletions: function (editor, session, pos, prefix, callback) {
                        if (prefix.length === 0 || justShowTable()) {
                            callback(null, []);
                        } else {
                            callback(null, $scope.srcColumns);
                        }
                    }
                });
                langTools.columnAdded = true;
            }

            _editor.commands.bindKey("Command-Option-Space", "startAutocomplete")
            _editor.setOptions({
                enableBasicAutocompletion: true
            });
        }

        $scope.parseQueryResult = function (oneQuery, result, status) {
            oneQuery.status = status;
            var data = [];

            angular.forEach(result.results, function (row, index) {
                var oneRow = {};
                angular.forEach(result.columnMetas, function (meta, metaIndex) {
                    oneRow[meta.label] = row[metaIndex];
                });
                data.push(oneRow);
            });

            var columnDefs = [];
            angular.forEach(result.columnMetas, function (meta, metaIndex) {
                columnDefs.push({field: meta.label, width: 120});
            });

            if (oneQuery.result.results) {
                oneQuery.result.results = oneQuery.result.results.concat(result.results);
            } else {
                oneQuery.result = result;
            }

            if (oneQuery.status == 'success') {
                if (oneQuery.result.data) {
                    oneQuery.result.data = oneQuery.result.data.concat(data);
                } else {
                    oneQuery.result.data = data;
                }
                angular.forEach(oneQuery.result.data,function(row,index){
                    angular.forEach(row,function(column,value){
                        var float =VdmUtil.SCToFloat(column);
                        if (float!=""){
                            oneQuery.result.data[index][value]=parseFloat(float);
                        }
                    });
                });
                $scope.curQuery.result.isResponsePartial = result.partial;
            }

            oneQuery.result.gridOptions = {
                data: 'curQuery.result.data',
                showGroupPanel: true,
                enablePinning: true,
                columnDefs: columnDefs
            };

            oneQuery.completeTime = new Date();
        }

        $scope.refreshUi = function () {
            $scope.ui.fullScreen = !$scope.ui.fullScreen;
            $timeout(function () {
                if ($scope.curQuery.result.gridOptions) {
                    $domUtilityService.RebuildGrid($scope.curQuery.result.gridOptions.$gridScope, $scope.curQuery.result.gridOptions.ngGrid);
                }
            });
        }

        $scope.sanitate = function (sql) {
            return encodeURIComponent(sql.replace(/\n/g, " "));
        }

        $scope.query = function (query) {
            scrollToButton();
            var backdoorToggles = null;
            if ($scope.specifyCube != null) {
              backdoorToggles = {"DEBUG_TOGGLE_HIT_CUBE": $scope.specifyCube};
            }
            QueryService.query({}, {sql: query.sql, offset: 0, limit: $scope.rowsPerPage, acceptPartial: query.acceptPartial, project: query.project, backdoorToggles: backdoorToggles}, function (result) {
                scrollToButton();
                $scope.parseQueryResult(query, result, (!result || result.isException) ? 'failed' : 'success');
                $scope.curQuery.result.hasMore = (query.result.results && query.result.results.length == $scope.rowsPerPage);
            }, function (result) {
                scrollToButton();
                $scope.parseQueryResult(query, result, 'failed');
            });
        }

        function scrollToButton() {
            $timeout(function () {
                $location.hash('query_content_results');
                $anchorScroll();
            });
        }

        $scope.submitQuery = function (queryString, project) {
            if (!queryString && !$scope.queryString) {
                return;
            }
            var sql = (!!queryString) ? queryString : $scope.queryString;
            var newQuery = Query.createNew(sql, project);
            $scope.queries.push(newQuery);
            $scope.curQuery = newQuery;
            $scope.query($scope.curQuery);
            $scope.cacheQuery($scope.curQuery);
            $scope.exportSql = $scope.sanitate(sql);
        }

        $scope.reset = function (query) {
            Query.resetQuery(query);
        }

        $scope.rerun = function () {
            Query.resetQuery($scope.curQuery);
            $scope.query($scope.curQuery);
        }

        $scope.loadMore = function (query) {
            query.result.loading = true;
            var query = query;

            QueryService.query({}, {sql: query.originSql, offset: query.result.results.length, limit: $scope.rowsPerPage, acceptPartial: query.acceptPartial, project: query.project}, function (result) {
                if (result && !result.isException) {
                    $scope.parseQueryResult($scope.curQuery, result, (!result || result.isException) ? 'failed' : 'success');
                }
                else {
                    query.status = 'failed';
                }
                query.result.hasMore = (result.results && result.results.length == $scope.rowsPerPage);
                query.result.loading = false;
            }, function (result) {
                query.status = 'failed';
            });
        }

        $scope.removeResult = function (queryInstance) {
            if ($scope.curQuery == queryInstance) {
                $scope.curQuery = null;
            }

            var index = $scope.queries.indexOf(queryInstance);
            $scope.queries.splice(index, 1);

            $scope.curQuery = ($scope.queries.length > 0) ? $scope.queries[0] : null;
        }

        $scope.removeQuery = function (queryInstance) {
            var queryToRemove = getQuery($scope.cachedQueries, this.query);

            if (queryToRemove) {
                var index = $scope.cachedQueries.indexOf(queryToRemove);
                $scope.cachedQueries.splice(index, 1);
                var indexFilter = $scope.cachedFilterQueries.indexOf(queryToRemove);
                $scope.cachedFilterQueries.splice(indexFilter, 1);
                storage.set("saved_queries", $scope.cachedQueries);
            }
        }

        $scope.cacheQuery = function (query) {
            if (!getQuery($scope.cachedQueries, query)) {

                if ($scope.cachedQueries.length >= 99) {
                    delete $scope.cachedQueries.splice(0, 1);
                }

                $scope.cachedQueries.push({
                    sql: query.sql,
                    savedAt: new Date(),
                    project: query.project
                });
                storage.set("saved_queries", $scope.cachedQueries);
            }
        }

        $scope.listSavedQueries = function () {
            QueryService.list({project: $scope.projectModel.selectedProject}, function (queries) {
                $scope.savedQueries = queries;
                $scope.savedQueries.curPage = 1;
                $scope.savedQueries.perPage = 3;
            });
        }

        $scope.listCachedQueries = function () {
          $scope.cachedQueries = storage.get("saved_queries") || [];
          $scope.cachedFilterQueries = $scope.cachedQueries.filter(function (query) {
            return query.project === $scope.projectModel.selectedProject;
          });
          if (!$scope.cachedFilterQueries) {
            $scope.cachedFilterQueries = [];
          }
          $scope.cachedFilterQueries.curPage = 1;
          $scope.cachedFilterQueries.perPage = 3;
        }

        $scope.removeSavedQuery = function (id) {
            QueryService.delete({subject_id: id}, function () {
                $scope.listSavedQueries();
            });
        }

        $scope.refreshCurQuery = function () {
            $scope.curQuery = this.query;
            $scope.curQuery.result = {
                isResponsePartial: false
            };
            $scope.query($scope.curQuery);
        };

        $scope.showSavePanel = function () {
            var modalInstance = $modal.open({
                templateUrl: 'saveQueryModal.html',
                controller: saveQueryController,
                resolve: {
                    curQuery: function () {
                        return $scope.curQuery;
                    }
                }
            });
            modalInstance.result.then( function (result) {
                $scope.listSavedQueries();
            }, function (reason) {
                $scope.listSavedQueries();
            });
        }

        var saveQueryController = function ($scope, $modalInstance, curQuery, QueryService) {
            $scope.curQuery = curQuery;

            $scope.cancel = function () {
                $modalInstance.dismiss('cancel');
            }

            $scope.saveQuery = function (query) {
                QueryService.save({}, {name: query.name, project: query.project, sql: query.sql, description: query.description}, function () {
                    SweetAlert.swal('Success!', 'New query saved..', 'success');
                    $modalInstance.dismiss('cancel');
                });
            }
        }

        $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
          $scope.listCachedQueries();
          $scope.listSavedQueries();
          $scope.cubeItems = [];
          $scope.specifyCube = null;
          CubeService.list({projectName:newValue}, function (_cubes) {
            if (_cubes !== 0) {
              angular.forEach(_cubes,function(cube){
                if (cube.status==="READY"){
                  $scope.cubeItems.push(cube.name);
                }
              })
            }
          })
        });

        $scope.$on('$locationChangeStart', function (event, next, current) {
            var isExecuting = false;
            var nextURL = $location.path();
            angular.forEach($scope.queries, function (query, index) {
                if (query.status == "executing") {
                    isExecuting = true;
                }
            });

            if (!$scope.locationChangeConfirmed && isExecuting && (next.replace(current, "").indexOf("#") != 0)) {
                event.preventDefault();
                SweetAlert.swal({
                  title: '',
                  text: "You've executing query in current page, are you sure to leave this page?",
                  type: '',
                  showCancelButton: true,
                  confirmButtonColor: '#DD6B55',
                  confirmButtonText: "Yes",
                  closeOnConfirm: true
                }, function(isConfirm) {
                    if(isConfirm){
                      $scope.locationChangeConfirmed = true;
                      $location.path(nextURL);
                    }
                });
            }
        });


    })
    .controller('QueryResultCtrl', function ($scope, storage, $base64, $q, $location, $anchorScroll, $routeParams, QueryService, kylinConfig, queryConfig) {
        $scope.isAdminExportAllowed = kylinConfig.isAdminExportAllowed();
        $scope.isNonAdminExportAllowed = kylinConfig.isNonAdminExportAllowed();
        $scope.buildGraphMetadata = function (query) {
            if (!query.graph.show) {
                return;
            }

            // Build graph metadata
            query.graph.meta.dimensions = [];
            query.graph.meta.metrics = [];
            var datePattern = /_date|_dt/i;
            query.graph.type = $scope.chartTypes[1];
            angular.forEach(query.result.columnMetas, function (meta, index) {
                if (($scope.dateTypes.indexOf(meta.columnType) > -1 || datePattern.test(meta.name))) {
                    query.graph.type = $scope.chartTypes[0];
                    query.graph.meta.dimensions.push({
                        column: meta,
                        index: index,
                        type: 'date'
                    });
                    return;
                }
                if ($scope.stringTypes.indexOf(meta.columnType) > -1) {
                    query.graph.meta.dimensions.push({
                        column: meta,
                        index: index,
                        type: 'string'
                    });
                    return;
                }
                if ($scope.numberTypes.indexOf(meta.columnType) > -1) {
                    query.graph.meta.metrics.push({
                        column: meta,
                        index: index
                    });
                    return;
                }
            });
        }

        $scope.mappingToChartype = function (dimension) {
            return $scope.curQuery.graph.type.dimension.types.indexOf(dimension.type) > -1;
        }

        $scope.resetGraph = function (query) {
            var dimension = (query.graph.meta.dimensions && query.graph.meta.dimensions.length > 0) ? query.graph.meta.dimensions[0] : null;
            var metrics = (query.graph.meta.metrics && query.graph.meta.metrics.length > 0) ? query.graph.meta.metrics[0] : null;
            query.graph.state = {
                dimensions: dimension,
                metrics: ((query.graph.type.metrics.multiple) ? [metrics] : metrics)
            };
            $scope.refreshGraphData(query);
        }

        $scope.refreshGraphData = function (query) {
            if (query.graph.show) {
                $scope.chart = undefined;

                var selectedDimension = query.graph.state.dimensions;
                var selectedMetric = query.graph.state.metrics;
                if (selectedDimension && selectedMetric && query.graph.type.dimension.types.indexOf(selectedDimension.type) > -1) {
                    $scope.chart = {};

                    var chartType = query.graph.type.value;

                    var dataValues = [];
                    angular.forEach(query.result.results, function(result, ind) {
                        var data = {
                            label: result[selectedDimension.index],
                            value: parseFloat(result[selectedMetric.index])
                        };
                        if (selectedDimension.type === 'date' && chartType === 'line') {
                            data.label = parseInt(moment(data.label).format('X'));
                        }
                        dataValues.push(data);
                    });

                    dataValues = _.sortBy(dataValues, 'label');
                    var oldLabel = dataValues[0].label;
                    var groupValues = [{label: dataValues[0].label, value: 0}];
                    angular.forEach(dataValues, function(data) {
                        if (data.label === oldLabel) {
                            groupValues[groupValues.length-1].value += data.value;
                        } else {
                            groupValues.push(data);
                            oldLabel = data.label;
                        }
                    });

                    $scope.chart.data = [{
                        key: selectedMetric.column.label,
                        values: groupValues
                    }];

                    if (chartType === 'line') {
                        $scope.chart.options = angular.copy(queryConfig.lineChartOptions);
                        if (selectedDimension.type === 'date') {
                            $scope.chart.options.chart.xAxis.tickFormat = function (d) {
                                return d3.time.format('%Y-%m-%d')(moment.unix(d).toDate());
                            };
                        }
                    } else if (chartType === 'bar') {
                        $scope.chart.options = angular.copy(queryConfig.barChartOptions);
                        if (groupValues.length > 15) {
                            $scope.chart.options.chart.showLegend = false;
                            $scope.chart.options.chart.xAxis.height = 100;
                            $scope.chart.options.chart.margin.bottom =  150;
                            $scope.chart.options.chart.xAxis.rotateLabels = -90;
                            if (groupValues.length > 50) {
                                $scope.chart.options.chart.showXAxis = false;
                            }
                        }
                    } else if (chartType === 'pie') {
                        $scope.chart.options = angular.copy(queryConfig.pieChartOptions);
                        $scope.chart.data = groupValues;
                        if (groupValues.length > 15) {
                            $scope.chart.options.chart.showLegend = false;
                            $scope.chart.options.chart.showLabels = false;
                        }
                    }
                }
            } else {
                $scope.chart.data = [];
            }
        }
    });
