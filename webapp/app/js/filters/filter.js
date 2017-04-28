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

/* Filters */
KylinApp
  .filter('orderObjectBy', function () {
    return function (input, attribute, reverse) {
      if (!angular.isObject(input)) return input;

      var array = [];
      for (var objectKey in input) {
        array.push(input[objectKey]);
      }

      array.sort(function (a, b) {
        if (!attribute) {
          return 0;
        }
        var result = 0;
        var attriOfA = a, attriOfB = b;
        var temps = attribute.split(".");
        if (temps.length > 1) {
          angular.forEach(temps, function (temp, index) {
            attriOfA = attriOfA[temp];
            attriOfB = attriOfB[temp];
          });
        }
        else {
          attriOfA = a[attribute];
          attriOfB = b[attribute];
        }

        if (!attriOfA) {
          result = -1;
        }
        else if (!attriOfB) {
          result = 1;
        }
        else {
          if(!isNaN(attriOfA)||!isNaN(attriOfB)){
            result = attriOfA > attriOfB ? 1 : attriOfA < attriOfB ? -1 : 0;
          }else{
            result = attriOfA.toLowerCase() > attriOfB.toLowerCase() ? 1 : attriOfA.toLowerCase() < attriOfB.toLowerCase() ? -1 : 0;
          }
        }
        return reverse ? -result : result;
      });
      return array;
    }
  })

  .filter('reverse', function () {
    return function (items) {
      if (items) {
        return items.slice().reverse();
      } else {
        return items;
      }
    }
  })
  .filter('range', function () {
    return function (input, total) {
      total = parseInt(total);
      for (var i = 0; i < total; i++)
        input.push(i);
      return input;
    }
  })
  // Convert bytes into human readable format.
  .filter('bytes', function () {
    return function (bytes, precision) {
      if (bytes === 0) {
        return 'less than 1 MB';
      }
      if (isNaN(parseFloat(bytes)) || !isFinite(bytes)) {
        return '-';
      }

      if (typeof precision === 'undefined') {
        precision = 3;
      }

      var units = ['bytes', 'kB', 'MB', 'GB', 'TB', 'PB'],
        number = Math.floor(Math.log(bytes) / Math.log(1024));
      switch(number){
        case 0:
          precision = 0;
          break;
        case 1:
        case 2:
          precision = 3;
          break;
        default:
          precision = 5;
      }

      return Math.round((bytes / Math.pow(1024, Math.floor(number)))*Math.pow(10,precision))/Math.pow(10,precision) + ' ' + units[number];
    }
  }).filter('resizePieHeight', function () {
    return function (item) {
      if (item < 150) {
        return 1300;
      }
      return 1300;
    }
  }).filter('utcToConfigTimeZone', function ($filter, kylinConfig) {

    var gmttimezone;
    //convert GMT+0 time to specified Timezone
    return function (item, timezone, format) {

      // undefined and 0 is not necessary to show
      if (angular.isUndefined(item) || item === 0) {
        return "";
      }

      if (angular.isUndefined(timezone) || timezone === '') {
        timezone = kylinConfig.getTimeZone() == "" ? 'PST' : kylinConfig.getTimeZone();
      }
      if (angular.isUndefined(format) || format === '') {
        format = "yyyy-MM-dd HH:mm:ss";
      }

      //convert short name timezone to GMT
      switch (timezone) {
        //convert PST to GMT
        case "PST":
          gmttimezone = "GMT-8";
          break;
        default:
          gmttimezone = timezone;
      }

      var localOffset = new Date().getTimezoneOffset();
      var convertedMillis = item;
      if (gmttimezone.indexOf("GMT+") != -1) {
        var offset = gmttimezone.substr(4, 1);
        convertedMillis = new Date(item).getTime() + offset * 60 * 60000 + localOffset * 60000;
      }
      else if (gmttimezone.indexOf("GMT-") != -1) {
        var offset = gmttimezone.substr(4, 1);
        convertedMillis = new Date(item).getTime() - offset * 60 * 60000 + localOffset * 60000;
      }
      else {
        // return PST by default
        timezone = "PST";
        convertedMillis = new Date(item).getTime() - 8 * 60 * 60000 + localOffset * 60000;
      }
      return $filter('date')(convertedMillis, format) + " " + timezone;

    }
  }).filter('reverseToGMT0', function ($filter) {
    //backend store GMT+0 timezone ,by default front will show local,so convert to GMT+0 Date String format
    return function (item) {
      if (item || item == 0) {
        item += new Date().getTimezoneOffset() * 60000;
        return $filter('date')(item, "yyyy-MM-dd HH:mm:ss");
      }
    }
  }).filter('millisecondsToDay', function ($filter) {
    //backend store GMT+0 timezone ,by default front will show local,so convert to GMT+0 Date String format
    return function (item) {
        return item/86400000;
    }
  }).filter('timeRangeFormat', function ($filter) {
    //backend store GMT+0 timezone ,by default front will show local,so convert to GMT+0 Date String format
    return function (item) {
      var _day = Math.floor(item/86400000);
      var _hour = (item%86400000)/3600000;
      if(_day==0){
        return _hour +" (Hours)"
      }else{
        return _day +" (Days)";
      }
    }
  }).filter('inDimNotInMea', function ($filter) {
    return function (inputArr, table, arr) {
      var out=[];
      angular.forEach(arr,function(item) {
        if (item.table == table) {
          angular.forEach(inputArr, function (inputItem) {
            if (item.columns.indexOf(inputItem.name) == -1) {
              out.push(inputItem);
            }
          });
        }
      });
      return out;
    }
  }).filter('inMeaNotInDim', function ($filter) {
        return function (inputArr, table, arr) {
          var out=[];
          angular.forEach(inputArr, function (inputItem) {
            if (arr.indexOf(table+"."+inputItem.name) == -1) {
              out.push(inputItem);
            }
          });
          return out;
        }
  }).filter('assignedMeasureNames', function ($filter) {
    //return the measures that haven't assign to column family
    return function (inputArr, assignedArr) {
      var out = [];
      angular.forEach(inputArr, function (inputItem) {
        if (assignedArr.indexOf(inputItem) == -1) {
          out.push(inputItem);
        }
      });
      return out;
    }
  });

