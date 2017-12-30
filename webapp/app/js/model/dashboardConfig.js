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

KylinApp.constant('dashboardConfig', {
  granularityFilter: [
    {name: 'Daily', value: 'day'},
    {name: 'Weekly', value: 'week'},
    {name: 'Monthly', value: 'month'}
  ],
  metrics: [
    {name: 'query count', value: 'QUERY_COUNT'},
    {name: 'avg query latency', value: 'AVG_QUERY_LATENCY'},
    {name: 'job count', value: 'JOB_COUNT'},
    {name: 'avg build time', value: 'AVG_JOB_BUILD_TIME'}
  ],
  dimensions: [
    {name: 'project', value: 'PROJECT'},
    {name: 'cube', value: 'CUBE'},
    {name: 'day', value: 'DAY'},
    {name: 'week', value: 'WEEK'},
    {name: 'month', value: 'MONTH'}
  ],
  categories: [
    'QUERY', 'JOB'
  ],
  baseChartOptions: {
    chart: {
      height: 272,
      margin : {
        top: 20,
        right: 40,
        bottom: 60,
        left: 45
      },
      useInteractiveGuideline: false,
      x: function(d){return d.label;},
      y: function(d){return d.value;},
      xAxis: {
        axisLabelDistance: 50,
        staggerLabels: false,
        tickFormat: function(d) {
          if (d.length > 10) {
            return d.substring(0,10) + '...';
          } else {
            return d;
          }
        }
      },
      yAxis: {
        tickFormat: function(d) {
          if (d < 1000) {
            if (parseFloat(d) === d) {
              return d3.format('.1')(d);
            } else {
              return d3.format('.2f')(d);
            }
          } else {
            var prefix = d3.formatPrefix(d);
            return prefix.scale(d) + prefix.symbol;
          } 
        },
        showMaxMin: false
      },
      valueFormat: function(d){
        return d3.format('.1')(d);
      },
      transitionDuration: 500,
      tooltip: {
        contentGenerator: function (d) {
          return '<table><tr><td class="legend-color-guide"><div style="background-color: '+d.color+';"></div></td><td class="key">' + d.data.label + '</td><td class="value">'+d.data.value.toFixed(2)+'</td></tr></table>';
        }
      }
    },
    title: {
      enable: true,
      css: {
        'margin-top': '20px'
      }
    }
  }
});