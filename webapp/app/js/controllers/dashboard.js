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

KylinApp.controller('DashboardCtrl', function ($scope, $location, storage, kylinConfig, dashboardConfig, DashboardService, MessageService, SweetAlert, loadingRequest, UserService, ProjectModel, $filter) {

  $scope.init = function(){
    $scope.timezone = 'GMT';
    // Init date range
    storage.bind($scope, 'dateRange', {defaultValue: {
      startDate: moment().subtract(7, 'days').clone().tz($scope.timezone).startOf('day').format('x'),
      endDate: moment().subtract(1, 'days').clone().tz($scope.timezone).endOf('day').format('x')
    }, storeName: 'dashboard.dateRange'});

    storage.bind($scope, 'barchartDimension', {defaultValue: dashboardConfig.dimensions[0], storeName: 'dashboard.barchart.dimension'});
    storage.bind($scope, 'barchartCategory', {defaultValue: dashboardConfig.categories[0], storeName: 'dashboard.barchart.category'});
    storage.bind($scope, 'barchartMetric', {defaultValue: dashboardConfig.metrics[0], storeName: 'dashboard.barchart.metric'});

    storage.bind($scope, 'linechartDimension', {defaultValue: dashboardConfig.dimensions[2], storeName: 'dashboard.linechart.dimension'});
    storage.bind($scope, 'linechartCategory', {defaultValue: dashboardConfig.categories[0], storeName: 'dashboard.linechart.category'});
    storage.bind($scope, 'linechartMetric', {defaultValue: dashboardConfig.metrics[0], storeName: 'dashboard.linechart.metric'});

    storage.bind($scope, 'currentSquare', 'queryCount');
    $scope.initReady = true;
  };

  $scope.formatDatetime = function(dateTime) {
    return moment(dateTime, 'x').tz($scope.timezone).format('YYYY-MM-DD');
  };

  $scope.refreshCubeMetric = function() {
    $scope.cubeMetricsRefreshTime = moment().tz($scope.timezone).format('YYYY-MM-DD');
    DashboardService.getCubeMetrics({projectName: ProjectModel.getSelectedProject(), cubeName: $scope.selectedCube}, {}, function (data) {
      $scope.cubeMetrics = data;
    }, function(e) {
        SweetAlert.swal('Oops...', 'Failed to load cube metrics', 'error');
        console.error('cube metrics error', e.data);
    });
  };

  // Need to change name for this function
  $scope.refreshOtherMetrics = function(){
    DashboardService.getQueryMetrics({projectName: ProjectModel.getSelectedProject(), cubeName: $scope.selectedCube, startTime: $scope.formatDatetime($scope.dateRange.startDate), endTime: $scope.formatDatetime($scope.dateRange.endDate)}, {}, function (data) {
      $scope.queryMetrics = data;
    }, function(e) {
        SweetAlert.swal('Oops...', 'Failed to load query metrics.', 'error');
        console.error('query metrics error:', e.data);
    });

    DashboardService.getJobMetrics({projectName: ProjectModel.getSelectedProject(), cubeName: $scope.selectedCube, startTime: $scope.formatDatetime($scope.dateRange.startDate), endTime: $scope.formatDatetime($scope.dateRange.endDate)}, {}, function (data) {
      $scope.jobMetrics = data;
    }, function(e) {
        SweetAlert.swal('Oops...', 'Failed to load job metrics.', 'error');
        console.error('job metrics error:', e.data);
    });

    $scope.createCharts();
  };

  // Daterangepicker call back
  $scope.changeDateRange = function(start, end) {
    console.log("start time:", start);
    console.log("end time:", end);
    $scope.dateRange.startDate = start;
    $scope.dateRange.endDate = end;
    $scope.refreshOtherMetrics();
  };

  // Create chart option and data
  $scope.createChart = function(dataQuery, chartType) {
    var chartObj = {
      queryObj: dataQuery
    };

    // get base options
    var baseOptions = dashboardConfig.baseChartOptions;

    var title = $filter('startCase')(dataQuery.metric.name) + ' by ' + $filter('startCase')(dataQuery.dimension.name);

    // set title to options
    chartObj.options = angular.copy(baseOptions);
    chartObj.options.chart.xAxis.axisLabel = dataQuery.dimension.name;
    chartObj.options.title.text = title;

    var groupByOptions = [];
    angular.forEach(dashboardConfig.granularityFilter, function(option) {
      groupByOptions.push(option.value);
    });
    if (groupByOptions.indexOf(dataQuery.dimension.name) > -1) {
      var formatPattern = '%Y-%m-%d';
      if (dataQuery.dimension.name === dashboardConfig.granularityFilter[2].value) {
        formatPattern = '%Y-%m';
      }
      chartObj.options.chart.xAxis.tickFormat = function (d) {
        return d3.time.format(formatPattern)(moment.unix(d/1000).toDate());
      };
      chartObj.options.chart.tooltip.contentGenerator = function (d) {
        return '<table><tr><td class="legend-color-guide"><div style="background-color: '+d.point.color+';"></div></td><td class="key">' + d3.time.format(formatPattern)(moment.unix(d.point.label/1000).toDate()) + '</td><td class="value">'+d.point.value.toFixed(2)+'</td></tr></table>';
      };

      // chartObj.options.chart.interpolate = 'cardinal';

      chartObj.options.chart.legend = {
        margin: {
          left: 15
        }
      };

      // Add filter for change month
      chartObj.dimension = {};
      chartObj.dimension.options = dashboardConfig.granularityFilter;
      chartObj.dimension.selected = dataQuery.dimension;
      angular.forEach(chartObj.dimension.options, function(option, ind) {
        if (dataQuery.dimension.name.indexOf(option.value) > -1) {
          chartObj.dimension.selected = chartObj.dimension.options[ind];
        }
      });
    }

    chartObj.data = [];

    if (chartType === 'line') {
      chartObj.options.chart.type = 'lineChart';
      $scope.lineChart = chartObj;
      DashboardService.getChartData({category: dataQuery.category, metric: dataQuery.metric.value, dimension: dataQuery.dimension.value, projectName: ProjectModel.getSelectedProject(), cubeName: $scope.selectedCube, startTime: $scope.formatDatetime($scope.dateRange.startDate), endTime: $scope.formatDatetime($scope.dateRange.endDate)}, {}, function (data) {
        if (data.length > 6) {
          $scope.lineChart.options.chart.xAxis.rotateLabels = -50;
          $scope.lineChart.options.chart.xAxis.axisLabel = '';
        }

        $scope.lineChart.data = [{key: dataQuery.category, values: _.sortBy(data, 'label')}];
      }, function(e) {
          SweetAlert.swal('Oops...', 'Failed to load line chart.', 'error');
          console.error('line chart error:', e.data);
      });
    } else  if (chartType === 'bar'){
      chartObj.options.chart.type = 'discreteBarChart';
      chartObj.options.chart.discretebar = {
        dispatch: {
          elementClick: function(el) {
            if (ProjectModel.getSelectedProject()) {
              $scope.selectedCube = el.data.label;
            } else {
              var project = el.data.label;
              ProjectModel.projects.forEach(function(pro) {
                if (pro.name.toLowerCase() === project.toLowerCase()) {
                  project = pro.name;
                }
              });
              ProjectModel.setSelectedProject(project);
            }
            $scope.$apply();
          }
        }
      };
      $scope.barChart = chartObj;
      DashboardService.getChartData({category: dataQuery.category, metric: dataQuery.metric.value, dimension: dataQuery.dimension.value, projectName: ProjectModel.getSelectedProject(), cubeName: $scope.selectedCube, startTime: $scope.formatDatetime($scope.dateRange.startDate), endTime: $scope.formatDatetime($scope.dateRange.endDate)}, {}, function (data) {
        if (data.length > 6) {
          $scope.barChart.options.chart.xAxis.rotateLabels = -50;
          $scope.barChart.options.chart.xAxis.axisLabel = '';
        }
        $scope.barChart.data = [{key: dataQuery.category, values: data}];
        if ($scope.selectedCube) {
          angular.forEach($scope.barChart.data[0].values, function (value, index){
            if (value.label != $scope.selectedCube) {
              value.color = '#ddd';
            }
          });
        }
      }, function(e) {
          SweetAlert.swal('Oops...', 'Failed to load bar chart.', 'error');
          console.error('bar chart error:', e.data);
      });
    }
  };

  // Clean and remove chart
  $scope.removeChart = function(chartType) {
    if (chartType === 'all') {
      $scope.barChart = undefined;
      $scope.lineChart = undefined;
    } else if (chartType == 'bar') {
      $scope.barChart = undefined;
    } else if (chartType == 'line') {
      $scope.lineChart = undefined;
    }
  };

  $scope.createCharts = function() {
    $scope.createChart({dimension: $scope.barchartDimension, category: $scope.barchartCategory, metric: $scope.barchartMetric}, 'bar');
    $scope.createChart({dimension: $scope.linechartDimension, category: $scope.linechartCategory, metric: $scope.linechartMetric}, 'line');
  };

  // Click query count square
  $scope.queryCountChart = function() {
    $scope.currentSquare = 'queryCount';
    $scope.barchartCategory = dashboardConfig.categories[0];
    $scope.barchartMetric = dashboardConfig.metrics[0];
    $scope.linechartCategory = dashboardConfig.categories[0];
    $scope.linechartMetric = dashboardConfig.metrics[0];

    $scope.removeChart('all');
    $scope.createCharts();
  };

  // Click avg query latency
  $scope.queryAvgChart = function() {
    $scope.currentSquare = 'queryAvg';
    $scope.barchartCategory = dashboardConfig.categories[0];
    $scope.barchartMetric = dashboardConfig.metrics[1];
    $scope.linechartCategory = dashboardConfig.categories[0];
    $scope.linechartMetric = dashboardConfig.metrics[1];

    $scope.removeChart('all');
    $scope.createCharts();
  };

  // Click job count
  $scope.jobCountChart = function() {
    $scope.currentSquare = 'jobCount';
    $scope.barchartCategory = dashboardConfig.categories[1];
    $scope.barchartMetric = dashboardConfig.metrics[2];
    $scope.linechartCategory = dashboardConfig.categories[1];
    $scope.linechartMetric = dashboardConfig.metrics[2];

    $scope.removeChart('all');
    $scope.createCharts();
  };

  // Click job count
  $scope.jobBuildTimeChart = function() {
    $scope.currentSquare = 'jobBuildTime';
    $scope.barchartCategory = dashboardConfig.categories[1];
    $scope.barchartMetric = dashboardConfig.metrics[3];
    $scope.linechartCategory = dashboardConfig.categories[1];
    $scope.linechartMetric = dashboardConfig.metrics[3];

    $scope.removeChart('all');
    $scope.createCharts();
  };

  // Line chart granularity change.
  $scope.changeDimensionFilter = function(chartType) {
    if (chartType === 'line') {
      var dataQuery = $scope.lineChart.queryObj;
      angular.forEach(dashboardConfig.dimensions, function(dimension, ind) {
        if (dimension.name === $scope.lineChart.dimension.selected.value) {
          dataQuery.dimension = dashboardConfig.dimensions[ind];
          $scope.linechartDimension = dashboardConfig.dimensions[ind];
        }
      });
      $scope.removeChart(chartType);
      $scope.createChart(dataQuery, chartType);
    }
  };

  // watch the project or cube change
  $scope.$watch('projectModel.selectedProject +"~"+ selectedCube', function (newValues, oldValues) {
    if ($scope.initReady) {
      if (ProjectModel.getSelectedProject() != null) {
        $scope.barchartDimension = dashboardConfig.dimensions[1];
      } else {
        $scope.barchartDimension = dashboardConfig.dimensions[0];
      }
      if (newValues.split('~')[0] != oldValues.split('~')[0]) {
        $scope.selectedCube = undefined;
      }
      $scope.refreshCubeMetric();
      $scope.refreshOtherMetrics();
    }
  });

  $scope.init();

  $scope.cleanSelectedCube = function() {
    $scope.selectedCube = undefined;
  };

});
