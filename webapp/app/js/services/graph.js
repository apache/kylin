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

KylinApp.service('GraphService', function (GraphBuilder, VdmUtil) {

  this.buildGraph = function (query) {
    var graphData = null;
    var dimension = query.graph.state.dimensions;

    if (dimension && query.graph.type.dimension.types.indexOf(dimension.type) > -1) {
      var metricsList = [];
      metricsList = metricsList.concat(query.graph.state.metrics);
      angular.forEach(metricsList, function (metrics, index) {
        var aggregatedData = {};
        angular.forEach(query.result.results,function(row,index){
          angular.forEach(row,function(column,value){
            var float = VdmUtil.SCToFloat(column);
              if (float!=""){
                query.result.results[index][value]=float;
              }
          });
        });
        angular.forEach(query.result.results, function (data, index) {
          aggregatedData[data[dimension.index]] = (!!aggregatedData[data[dimension.index]] ? aggregatedData[data[dimension.index]] : 0)
          + parseFloat(data[metrics.index].replace(/[^\d\.\-]/g, ""));
        });

        var newData = GraphBuilder["build" + capitaliseFirstLetter(query.graph.type.value) + "Graph"](dimension, metrics, aggregatedData);
        graphData = (!!graphData) ? graphData.concat(newData) : newData;
      });
    }

    return graphData;
  }

  function capitaliseFirstLetter(string) {
    return string.charAt(0).toUpperCase() + string.slice(1);
  }
});
