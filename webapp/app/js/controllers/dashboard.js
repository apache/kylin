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

KylinApp.controller('DashBoardCtrl', function ($scope, DashBoardService, $log, $q) {


    $scope.stastic = {
        countUser: 0,
        last30DayPercentile: 0,
        cubeStorage: 0,
        avgDayQuery: 0,
        cubesCount: 0
    }

    $scope.eachDayPercentileData=[];


    $scope.reduceCubeSourceTicks=false;

    // each day percentile chart
    $scope.cubeInfo = function () {
      var cubeSourceRecords ={"key":"Cube Source Records","values":[],"sizes":[]};
      $scope.cubeInfoPromise = DashBoardService.listCubes({},
            function (data) {
                if(data.length>30){
                    $scope.reduceCubeSourceTicks=true;
                }
                for (var i = 0; i < data.length; i++) {
                  cubeSourceRecords.values.push([data[i].name,parseInt(data[i].input_records_count)]);
                  cubeSourceRecords.sizes.push((data[i].size_kb));
                   $log.info(data[i]);
                }
              $scope.cubeUsageData = [cubeSourceRecords];

            }, function (result) {
                $log.error(result);
            }).$promise;
    }();


    $scope.cubeSourceYAxisTickFormat = function(){
        return d3.format('0');
    }

    $scope.avgDayQuery = function () {
        $scope.avgDayQueryPromise = DashBoardService.avgDayQuery({},
            function (data) {
                if (!isNaN(data[0][0])) {
                    $scope.stastic.avgDayQuery = Math.round(data[0][0]);
                } else {
                    $log.info("No data available.");
                }
                $log.info("avg day query:" + data);
            }, function (result) {
                $log.error(result);
            }).$promise;
    }();

    $scope.totalQueryUser = function () {
        $scope.queryUserPromise = DashBoardService.totalQueryUser({},
            function (data) {
                if (!isNaN(data[0][0])) {
                    $scope.stastic.userCount = data[0][0];
                } else {
                    $log.info("No data available.");
                }

                $log.info("total query user:" + data);
            }, function (result) {
                $log.error(result);
            }).$promise;
    }();

    $scope.last30DayPercentile = function () {
        $scope.last30DayPercentilePromise = DashBoardService.last30DayPercentile({},
            function (data) {
                if (!isNaN(data[0][0])) {
                    $scope.stastic.last30DayPercentile = Math.round(data[0][0] * 100) / 100;
                } else {
                    $log.info("No data available.");
                }
                $log.info("last 30 Day 90th Percentile:" + data);
            }, function (result) {
                $log.error(result);
            }).$promise;
    }();


  //daily query num
  $scope.dailyQueryData = [];


  // last 30 days
  $scope.dailyQueryCount = function () {
    var queryCount ={"key":"Query Count","bar":true,"values":[]};
    $scope.dailyQueryCountPromise = DashBoardService.dailyQueryCount({},
      function (data) {
        for (var i = 0; i < data.length; i++) {
          $scope.dailyQueryData.push(parseInt(data[i][1]));
          queryCount.values.push([new Date(data[i][0]).getTime()/1000,data[i][1]]);
        }
        console.log("daily query count");
        $scope.eachDayPercentileData.push(queryCount);
      }, function (result) {
        $log.error(result);
      }).$promise;
  };

    $scope.eachDayPercentile = function () {
        var percenTile90 ={"key":"90%-ile","values":[]},percenTile95 = {"key":"95%-ile","values":[]},queryCount ={"key":"Query Count","bar":true,"values":[]};;
        $scope.eachDayPercentilePromise = DashBoardService.eachDayPercentile({},
            function (data) {
                for (var i = 0; i < data.length; i++) {
                    var _latency90 = data[i][1].split(",")[0].substr(1);
                    var _latency95 = data[i][1].split(",")[1].substr(0, data[i][1].length - 1);
                    var _querycount = data[i][2];
                    percenTile90.values.push([new Date(data[i][0]).getTime()/1000,Math.round(parseFloat(_latency90) * 100) / 100]);
                    percenTile95.values.push([new Date(data[i][0]).getTime()/1000,Math.round(parseFloat(_latency95) * 100) / 100]);
                    queryCount.values.push([new Date(data[i][0]).getTime()/1000,Math.round(parseInt(_querycount) * 100) / 100]);
                }
                 $scope.eachDayPercentileData=[percenTile90,percenTile95,queryCount];
             }, function (result) {
                $log.error(result);
            }).$promise;
    }();


  $scope.dailyLatencyReportRightYaxis = function(){
    return "Query Latency";
  }

  $scope.xAxisTickFormatFunction = function(){
    return function(d){
      return d3.time.format('%Y-%m-%d')(moment.unix(d).toDate());
    }
  };


    $scope.legendColorFunction = function(){
        return function(d){
            return '#E01B5D';
        }
    };


    $scope.reduceProjectPercentileTicks=false;

    $scope.projectPercentile = function () {

        var percenTile90 ={"key":"90%-ile","values":[]},percenTile95 = {"key":"95%-ile","values":[]};


        $scope.projectPercentilePromise = DashBoardService.projectPercentile({},
            function (data) {
                if(data.length>30){
                    $scope.reduceProjectPercentileTicks=true;
                }
                for (var i = 0; i < data.length; i++) {

                    var _latency90 = data[i][1].split(",")[0].substr(1);
                    var _latency95 = data[i][1].split(",")[1].substr(0, data[i][1].length - 1);
                    percenTile90.values.push([data[i][0],Math.round(parseFloat(_latency90) * 100) / 100]);
                    percenTile95.values.push([data[i][0],Math.round(parseFloat(_latency95) * 100) / 100]);

                }
                $scope.eachProjectPercentileData=[percenTile90,percenTile95];
            }, function (result) {
                $log.error(result);
            }).$promise;
    }();

  $scope.projectPercentileToolTipContentFunction = function(key, x, y, e, graph) {
    return '<table>'+'<tr>'+'<td>'+'<strong>Latency</strong>'+'</td>'+'<td>'+' : '+y+'(S)'+'</td>'+'</tr>'+'<tr>'+'<td>'+'<strong>Percentile</strong>'+'</td>'+'<td>'+' : '+key+'</td>'+'</tr>'+'<tr>'+'<td>'+'<strong>Project</strong>'+'</td>'+'<td>'+' : '+x+'</td>'+'</tr>'+'</table>';

  }

  $scope.dailyPercentileToolTip = function(key, x, y, e, graph) {
      var suffix ='';
      if(key.indexOf('ile')!=-1){
          suffix = '(S)';
      }
      return '<table>'+'<tr>'+'<td>'+'<strong>'+key+'</strong>'+'</td>'+'<td>'+' : '+y+suffix+'</td>'+'</tr>'+'<tr>'+'<td>'+'<strong>Date</strong>'+'</td>'+'<td>'+' : '+x+'</td>'+'</tr>'+'</table>';

  }
  $scope.cubeToolTipContentFunction = function(key, x, y, e, graph) {
    return '<table>'+'<tr>'+'<td>'+'<strong>Source Records</strong>'+'</td>'+'<td>'+' : '+parseInt(y)+'</td>'+'</tr>'+'<tr>'+'<td>'+'<strong>Source Size</strong>'+'</td>'+'<td>'+' : '+$scope.dataSize(e.series.sizes[e.pointIndex]*1024)+'</td>'+'</tr>'+'<tr>'+'<td>'+'<strong>Cube Name</strong>'+'</td>'+'<td>'+' : '+x+'</td>'+'</tr>'+'</table>';
  }

  $scope.commonToolTipContentFunction = function(key, x, y, e, graph) {
    return '<p>' +  x +':'+y+ '</p>';
  }

    $scope.cubesStorage = function () {
        $scope.cubesStoragePromise = DashBoardService.cubesStorage({},
            function (cubes) {
                $scope.stastic.cubesCount = cubes.length;
                var _cubeStorage = $scope.getTotalSize(cubes);
                $scope.stastic.cubeStorage = _cubeStorage;
            }, function (result) {
                $log.error(result);
            }).$promise;
    }();

    $scope.getTotalSize = function (cubes) {
        var size = 0;
        if (!cubes) {
            return 0;
        }
        else {
            for (var i = 0; i < cubes.length; i++) {
                size += cubes[i].size_kb;
            }
            return $scope.dataSize(size * 1024);
        }
    };

});



