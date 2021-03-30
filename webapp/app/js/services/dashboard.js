/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

KylinApp.factory('DashboardService', ['$resource', '$location', function ($resource, $location, config) {
  return $resource(Config.service.url + 'dashboard/:type/:category/:metric/:dimension', {}, {
      getCubeMetrics: {
        method: 'GET',
        params: {type: 'metric', category: 'cube'},
        isArray: false,
        interceptor: {
          response: function(response) {
            var data = response.data;
            var cubeMetrics;
            if (data) {
              cubeMetrics = {totalCubes: data.totalCube, expansionRate: {avg: data.avgCubeExpansion, max: data.maxCubeExpansion, min: data.minCubeExpansion}};
            }
            return cubeMetrics;
          }
        }
      },
      getQueryMetrics: {
        method: 'GET',
        params: {type: 'metric', category: 'query'},
        isArray: false,
        interceptor: {
          response: function(response) {
            var data = response.data;
            var queryMetrics;
            if (data) {
              queryMetrics =  {queryCount: data.queryCount, queryLatency: {avg: data.avgQueryLatency/1000, max: data.maxQueryLatency/1000, min: data.minQueryLatency/1000}};
            }
            return queryMetrics;
          }
        }
      },
      getJobMetrics: {
        method: 'GET', 
        params: {type: 'metric', category: 'job'},
        isArray: false,
        interceptor: {
          response: function(response) {
            var data = response.data;
            var jobMetrics;
            if (data) {
              jobMetrics = {jobCount: data.jobCount, buildingTime: {avg: data.avgJobBuildTime*1024*1024/1000, max: data.maxJobBuildTime*1024*1024/1000, min: data.minJobBuildTime*1024*1024/1000}};
            }
            return jobMetrics;
          }
        }
      },
      getChartData: {
        method: 'GET',
        params: {type: 'chart'},
        isArray: false,
        interceptor: {
          response: function(response) {
            var data = response.data;
            var chartValues = [];
            if (data) {
              angular.forEach(Object.keys(data), function(key) {
                var label = key;
                var regEx = /^\d{4}-\d{1,2}-\d{1,2}$/;
                if(key.match(regEx)) {
                  label = moment(key, 'YYYY-MM-DD').format('x');
                }
                var value = data[key];
                if (response.config.url.indexOf('/JOB/AVG_JOB_BUILD_TIME') > -1) { // AVG Job Build time format ms to sec
                  value = value*1024*1024/1000;
                } else if (response.config.url.indexOf('/QUERY/AVG_QUERY_LATENCY') > -1) { // AVG Query Latency format ms to sec
                  value = value/1000;
                }
                chartValues.push({'label': label, 'value': value});
              });
            }
            return chartValues;
          }
        }
      }
    });
}]);
