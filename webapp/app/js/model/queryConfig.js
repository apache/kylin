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

KylinApp.constant('queryConfig', {
  lineChartOptions: {
    chart: {
      type: 'lineChart',
      height: 500,
      margin : {
        top: 20,
        right: 55,
        bottom: 60,
        left: 55
      },
      useInteractiveGuideline: true,
      interpolate: 'cardinal',
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
      tooltipContent: function (key, x, y, e, graph) {
        return '<h3>' + e.point.label + '</h3>' + '<p>' +  y + '</p>';
      }
    }
  },
  barChartOptions: {
    chart: {
      type: 'discreteBarChart',
      height: 500,
      margin : {
        top: 20,
        right: 20,
        bottom: 60,
        left: 55
      },
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
      tooltipContent: function (key, x, y, e, graph) {
        return '<h3>' + e.point.label + '</h3>' + '<p>' +  y + '</p>';
      }
    }
  },
  pieChartOptions: {
    chart: {
      type: 'pieChart',
      height: 500,
      showLabels: true,
      duration: 500,
      labelThreshold: 0.01,
      labelSunbeamLayout: true,
      legend: {
        margin : {
          top: 20,
          right: 20,
          bottom: 60,
          left: 55
        }
      },
      x: function(d){return d.label;},
      y: function(d){return d.value;},
      valueFormat: function(d){
        return d3.format('.1')(d);
      }
    }
  }
});