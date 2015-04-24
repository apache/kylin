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

KylinApp.factory('GraphBuilder', function () {
  var graphBuilder = {};

  graphBuilder.buildLineGraph = function (dimension, metrics, aggregatedData) {
    var values = [];
    angular.forEach(getSortedKeys(aggregatedData), function (sortedKey, index) {
      values.push([(dimension.type == 'date') ? moment(sortedKey).unix() : sortedKey, aggregatedData[sortedKey]]);
    });

    var newGraph = [
      {
        "key": metrics.column.label,
        "values": values
      }
    ];

    return newGraph;
  }

  graphBuilder.buildBarGraph = function (dimension, metrics, aggregatedData) {
    var newGraph = [];
    angular.forEach(getSortedKeys(aggregatedData), function (sortedKey, index) {
      newGraph.push({
        key: sortedKey,
        values: [
          [sortedKey, aggregatedData[sortedKey]]
        ]
      });
    });

    return newGraph;
  }

  graphBuilder.buildPieGraph = function (dimension, metrics, aggregatedData) {
    var newGraph = [];
    angular.forEach(getSortedKeys(aggregatedData), function (sortedKey, index) {
      newGraph.push({
        key: sortedKey,
        y: aggregatedData[sortedKey]
      });
    });

    return newGraph;
  }

  function getSortedKeys(results) {
    var sortedKeys = [];
    for (var k in results) {
      if (results.hasOwnProperty(k)) {
        sortedKeys.push(k);
      }
    }
    sortedKeys.sort();

    return sortedKeys;
  }

  return graphBuilder;
});
