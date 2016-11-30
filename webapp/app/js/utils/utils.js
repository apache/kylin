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

/* utils */

KylinApp.factory('VdmUtil', function ($modal, $timeout, $location, $anchorScroll, $window) {
  return {
    createDialog: function (template, scope, thenFunc, options) {
      options = (!!!options) ? {} : options;
      options = angular.extend({
        backdropFade: true,
        templateUrl: template,
        resolve: {
          scope: function () {
            return scope;
          }
        },
        controller: function ($scope, $modalInstance, scope) {
          $scope = angular.extend($scope, scope);
          $scope.animate = "fadeInRight";
          $scope.close = function (data) {
            $scope.animate = "fadeOutRight";
            $timeout(function () {
              $modalInstance.close(data);
            }, 500);
          }
        }
      }, options);

      var dialog = $modal.open(options);
      dialog.result.then(thenFunc);
    },

    formatDate: function (date, fmt) {
      var o = {
        "M+": date.getMonth() + 1,
        "d+": date.getDate(),
        "h+": date.getHours(),
        "m+": date.getMinutes(),
        "s+": date.getSeconds(),
        "q+": Math.floor((date.getMonth() + 3) / 3),
        "S": date.getMilliseconds()
      };
      if (/(y+)/.test(fmt)) fmt = fmt.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
      for (var k in o)
        if (new RegExp("(" + k + ")").test(fmt)) fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));

      return fmt;
    },

    SCToFloat:function(data){
      var resultValue = "";
      if (data&&data.indexOf('E') != -1){
        var regExp = new RegExp('^((\\d+.?\\d+)[Ee]{1}(\\d+))$', 'ig');
        var result = regExp.exec(data);
        var power = "";
        if (result != null){
          resultValue = result[2];
          power = result[3];
        }
        if (resultValue != ""){
          if (power != ""){
            var powVer = Math.pow(10, power);
            resultValue = (resultValue * powVer).toFixed(2);
          }
        }
      }
      return resultValue;
    },
    getFilterObjectListByAndFilterVal:function(objList,key,value,matchkey,matchval){
       var len=objList&&objList.length|| 0,newArr=[];
       for(var i=0;i<len;i++){
          if(!key||value===objList[i][key]||(angular.isArray(value)&&value.indexOf(objList[i][key])>-1)){
             if(matchkey){
               if(matchval==objList[i][matchkey]||(angular.isArray(matchval)&&value.indexOf(objList[i][matchkey])>-1)){
                 newArr.push(objList[i])
               }
             }else{
               newArr.push(objList[i])
             }
          }
       }
      return newArr;
    },
    getFilterObjectListByOrFilterVal:function(objList,key,val,orKey,orVal){
      var len=objList&&objList.length|| 0,newArr=[];
      for(var i=0;i<len;i++){
        if((key&&val===objList[i][key])||(orKey&&objList[i][orKey]===orVal)){
          newArr.push(objList[i]);
        }
      }
      return newArr;
    },
    removeFilterObjectList:function(objList,key,val,orKey,orVal){
      var len=objList&&objList.length|| 0,newArr=[];
      for(var i=0;i<len;i++){
        if(key&&val!=objList[i][key]){
          newArr.push(objList[i]);
        }
      }
      return newArr;
    },
    //过滤对象中的空值
    filterNullValInObj:function(needFilterObj){
      var newFilterObj,newObj;
      if(typeof needFilterObj=='string'){
        newObj=angular.fromJson(needFilterObj);
      }else{
        newObj=angular.extend({},needFilterObj);
      }
      function filterData(data){
        var obj=data;
        for(var i in obj){
          if(obj[i]===null){
            if(Object.prototype.toString.call(obj)=='[object Object]'){
              delete obj[i];
            }
          }
          else if(typeof obj[i]=== 'object'){
            obj[i]=filterData(obj[i]);
          }
        }
        return obj;
      }
      return angular.toJson(filterData(newObj),true);
    }

  }
});
