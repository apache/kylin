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

/* Directives */

KylinApp.directive('kylinPagination', function ($parse, $q) {
  return {
    restrict: 'E',
    scope: {},
    templateUrl: 'partials/directives/pagination.html',
    link: function (scope, element, attrs) {
      var _this = this;
      scope.limit = 15;
      scope.hasMore = false;
      scope.data = $parse(attrs.data)(scope.$parent);
      scope.action = $parse(attrs.action)(scope.$parent);
      scope.loadFunc = $parse(attrs.loadFunc)(scope.$parent);
      scope.autoLoad = true;


      scope.$watch("action.reload", function (newValue, oldValue) {
        if (newValue != oldValue) {
          scope.reload();
        }
      });

      var autoLoad = $parse(attrs.autoLoad)(scope.$parent);
      if (autoLoad == false) {
        scope.autoLoad = autoLoad;
      }

      scope.getLength = function (object) {
        if (!object) {
          return 0;
        }
        if (Object.prototype.toString.call(object) === '[object Array]') {
          return object.length;
        }
        else {
          var size = 0, key;
          for (key in object) {
            if (object.hasOwnProperty(key) && key != 'reload') size++;
          }

          return size;
        }
      }

      scope.reload = function () {
        var length = scope.getLength(scope.data);
        scope.loadFunc(0, scope.limit).then(function (dataLength) {
          scope.data = $parse(attrs.data)(scope.$parent);
          scope.hasMore = dataLength == scope.limit;

          return scope.data;
        });
      }

      if (scope.autoLoad) {
        scope.reload();
      }

      scope.loaded = true;
      return scope.showMore = function () {
        var loadPromise,
          _this = this;
        scope.loaded = false;
        var promises = [];
        var length = scope.getLength(scope.data);
        loadPromise = scope.loadFunc(length, scope.limit).then(function (dataLength) {
          scope.data = $parse(attrs.data)(scope.$parent);
          scope.hasMore = (dataLength == scope.limit);

          return scope.data;
        });
        promises.push(loadPromise);

        return $q.all(promises).then(function () {
          return scope.loaded = true;
        });
      };
    }
  };
})
  .directive('loading', function ($parse, $q) {
    return {
      restrict: 'E',
      scope: {},
      templateUrl: 'partials/directives/loading.html',
      link: function (scope, element, attrs) {
        scope.text = (!!!attrs.text) ? 'Loading...' : attrs.text;
      }
    };
  })
  .directive('noResult', function ($parse, $q) {
    return {
      scope: {},
      templateUrl: 'partials/directives/noResult.html',
      link: function (scope, element, attrs) {
        scope.text = (!!!attrs.text) ? 'No Result.' : attrs.text;
      }
    };
  }).directive('showonhoverparent',
  function() {
    return {
      link : function(scope, element, attrs) {
        element.parent().bind('mouseenter', function(e) {
          e.stopPropagation();
          element.show();
        });
        element.parent().bind('mouseleave', function(e) {
          e.stopPropagation();
          element.hide();
        });
      }
    };
  })
  .directive('typeahead', function ($timeout, $filter) {
    return {
      restrict: 'AEC',
      scope: {
        items: '=',
        prompt: '@',
        title: '@',
        model: '=',
        required: '@'
      },
      templateUrl: 'partials/directives/typeahead.html',
      link: function (scope, elem, attrs) {
        scope.current = null;
        scope.selected = true; // hides the list initially

        scope.handleSelection = function () {
          scope.model = scope.current[scope.title];
          scope.current = null;
          scope.selected = true;
        };
        scope.isCurrent = function (item) {
          return scope.current == item;
        };
        scope.setCurrent = function (item) {
          scope.current = item;
        };
        scope.keyListener = function (event) {
          var list, idx;
          switch (event.keyCode) {
            case 13:
              scope.handleSelection();
              break;
            case 38:
              list = $filter('filter')(scope.items, {name: scope.model});
              scope.candidates = $filter('orderBy')(list, 'name');
              idx = scope.candidates.indexOf(scope.current);
              if (idx > 0) {
                scope.setCurrent(scope.candidates[idx - 1]);
              } else if (idx == 0) {
                scope.setCurrent(scope.candidates[scope.candidates.length - 1]);
              }
              break;
            case 40:
              list = $filter('filter')(scope.items, {name: scope.model});
              scope.candidates = $filter('orderBy')(list, 'name');
              idx = scope.candidates.indexOf(scope.current);
              if (idx < scope.candidates.length - 1) {
                scope.setCurrent(scope.candidates[idx + 1]);
              } else if (idx == scope.candidates.length - 1) {
                scope.setCurrent(scope.candidates[0]);
              }
              break;
            default:
              break;
          }
        };

      }
    };
  })
  .directive('autoFillSync', function ($timeout) {
    return {
      require: 'ngModel',
      link: function (scope, elem, attrs, ngModel) {
        var origVal = elem.val();
        $timeout(function () {
          var newVal = elem.val();
          if (ngModel.$pristine && origVal !== newVal) {
            ngModel.$setViewValue(newVal);
          }
        }, 500);
      }
    }
  }).directive('retentionFormat', function() {
    return {
      require: 'ngModel',
      link: function(scope, element, attrs, ngModelController) {
        ngModelController.$parsers.push(function(data) {
          //convert data from view format to model format
          return data*86400000; //converted
        });

        ngModelController.$formatters.push(function(data) {
          //convert data from model format to view format
          return data/86400000; //converted
        });
      }
    }
  }).directive('datepickerTimezone', function () {
    // this directive workaround to convert GMT0 timestamp to GMT date for datepicker
    return {
      restrict: 'A',
      priority: 1,
      require: 'ngModel',
      link: function (scope, element, attrs, ctrl) {
        ctrl.$formatters.push(function (value) {

          //set null for 0
          if(value===0){
            return null;
          }

          //return value;
          var date = new Date(value + (60000 * new Date().getTimezoneOffset()));
          return date;
        });

        ctrl.$parsers.push(function (value) {
          if (isNaN(value)||value==null) {
            return value;
          }
          value = new Date(value.getFullYear(), value.getMonth(), value.getDate(), 0, 0, 0, 0);
          return value.getTime()-(60000 * value.getTimezoneOffset());
        });
      }
    };
  }).directive('dateTimepickerTimezone', function () {
    return {
      restrict: 'A',
      priority: 1,
      require: 'ngModel',
      link: function (scope, element, attrs, ctrl) {
        ctrl.$formatters.push(function (value) {

          //set null for 0
          if(value===0){
            return '';
          }

          //return value;
          var newDate = new Date(value);
          var year = newDate.getUTCFullYear();
          var month = (newDate.getUTCMonth()+1)<10?'0'+(newDate.getUTCMonth()+1):(newDate.getUTCMonth()+1);
          var date = newDate.getUTCDate()<10?'0'+newDate.getUTCDate():newDate.getUTCDate();
          var hour = newDate.getUTCHours()<10?'0'+newDate.getUTCHours():newDate.getUTCHours();
          var mins = newDate.getUTCMinutes()<10?'0'+newDate.getUTCMinutes():newDate.getUTCMinutes();
          var seconds = newDate.getUTCSeconds()<10?'0'+newDate.getUTCSeconds():newDate.getUTCSeconds();
          var viewVal = year+"-"+month+"-"+date+" "+hour+":"+mins+":"+seconds;
          return viewVal;
        });

        ctrl.$parsers.push(function (value) {
          var date;
          if(/^\d{4}-\d{1,2}-\d{1,2}(\s+\d{1,2}:\d{1,2}:\d{1,2})?$/.test(value)) {
            date=new Date(value);
            if(!date||date&&!date.getTime()){
              return value;
            }else{
              var dateSplit=value.replace(/^\s+|\s+$/,'').replace(/\s+/,'-').split(/[:-]/);
              var resultDate=[];
              for(var i=0;i<6;i++){
                resultDate[i]=dateSplit[i]||0;
              }
              return Date.UTC(resultDate[0],resultDate[1]-1,resultDate[2],resultDate[3],resultDate[4],resultDate[5]);
            }
          }else{
            return value;
          }
        });
      }
    };
}).directive("parametertree", function($compile) {
    return {
      restrict: "E",
      transclude: true,
      scope: {
        nextpara: '='
      },
      template:
      '<li class="parent_li">Value:<b>{{nextpara.value}}</b>, Type:<b>{{ nextpara.type }}</b></li>' +
      '<parametertree ng-if="nextpara.next_parameter!=null" nextpara="nextpara.next_parameter"></parameterTree>',
      compile: function(tElement, tAttr, transclude) {
        var contents = tElement.contents().remove();
        var compiledContents;
        return function(scope, iElement, iAttr) {
          if(!compiledContents) {
            compiledContents = $compile(contents, transclude);
          }
          compiledContents(scope, function(clone, scope) {
            iElement.append(clone);
          });
        };
      }
    };
  }).directive("groupbytree", function($compile) {
    return {
      restrict: "E",
      transclude: true,
      scope: {
        nextpara: '=',
      },
      template:
      '<b>{{nextpara.value}}<b ng-if="nextpara.next_parameter!=null">,</b></b>'+
      '<groupbytree ng-if="nextpara.next_parameter!=null" nextpara="nextpara.next_parameter"></groupbytree>',
      compile: function(tElement, tAttr, transclude) {
        var contents = tElement.contents().remove();
        var compiledContents;
        return function(scope, iElement, iAttr) {
          if(!compiledContents) {
            compiledContents = $compile(contents, transclude);
          }
          compiledContents(scope, function(clone, scope) {
            iElement.append(clone);
          });
        };
      }
    };
  }).directive("topntree", function($compile) {
  return {
    restrict: "E",
    transclude: true,
    scope: {
      nextpara: '='
    },
    template:
    '<li class="parent_li">SUM|ORDER BY:<b>{{nextpara.value}}</b></b></li>' +
    '<li class="parent_li">Group By:'+
    '<groupbytree nextpara="nextpara.next_parameter"></groupbytree>'+
    '</li>',
    compile: function(tElement, tAttr, transclude) {
      var contents = tElement.contents().remove();
      var compiledContents;
      return function(scope, iElement, iAttr) {
        if(!compiledContents) {
          compiledContents = $compile(contents, transclude);
        }
        compiledContents(scope, function(clone, scope) {
          iElement.append(clone);
        });
      };
    }
  };
}).directive("extendedcolumntree", function($compile) {
  return {
    restrict: "E",
    transclude: true,
    scope: {
      nextpara: '='
    },
    template:
    '<li class="parent_li">Host Column:<b>{{nextpara.value}}</b></b></li>' +
    '<li class="parent_li">Extended Column:<b>{{nextpara.next_parameter.value}}</b></li>',
    compile: function(tElement, tAttr, transclude) {
      var contents = tElement.contents().remove();
      var compiledContents;
      return function(scope, iElement, iAttr) {
        if(!compiledContents) {
          compiledContents = $compile(contents, transclude);
        }
        compiledContents(scope, function(clone, scope) {
          iElement.append(clone);
        });
      };
    }
  };
}).directive('kylinpopover', function ($compile,$templateCache) {
  return {
    restrict: "A",
    link: function (scope, element, attrs) {
      var popOverContent;
      var dOptions = {
        placement : 'right'
      }
      popOverContent = $templateCache.get(attrs.template);

      var placement = attrs.placement? attrs.placement : dOptions.placement;
      var title = attrs.title;

      var options = {
        content: popOverContent,
        placement: placement,
        trigger: "hover",
        title: title,
        html: true
      };
      $(element).popover(options);
    }
  };
}).directive('extendedColumnReturn', function() {
  return {
    require: 'ngModel',
    link: function(scope, element, attrs, ngModelController) {

      var prefix = "extendedcolumn(";
      var suffix = ")";
      ngModelController.$parsers.push(function(data) {
        //convert data from view format to model format
        return prefix +data+suffix; //converted
      });

      ngModelController.$formatters.push(function(data) {
        //convert data from model format to view format
        var prefixIndex = data.indexOf("(")+1;
        var suffixIndex = data.indexOf(")");
        return data.substring(prefixIndex,suffixIndex); //converted
      });
    }
  }
});
