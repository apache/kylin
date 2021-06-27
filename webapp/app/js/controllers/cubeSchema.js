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

KylinApp.controller('CubeSchemaCtrl', function ($scope, QueryService, UserService,modelsManager, ProjectService, AuthenticationService,$filter,ModelService,MetaModel,CubeDescModel,CubeList,TableModel,ProjectModel,ModelDescService,SweetAlert,cubesManager,StreamingService,CubeService,VdmUtil,tableConfig,$q) {
    $scope.modelsManager = modelsManager;
    $scope.cubesManager = cubesManager;
    $scope.projects = [];
    $scope.newDimension = null;
    $scope.newMeasure = null;
    $scope.forms={};
    $scope.wizardSteps = [
        {title: 'Cube Info', src: 'partials/cubeDesigner/info.html', isComplete: false,form:'cube_info_form'},
        {title: 'Dimensions', src: 'partials/cubeDesigner/dimensions.html', isComplete: false,form:'cube_dimension_form'},
        {title: 'Measures', src: 'partials/cubeDesigner/measures.html', isComplete: false,form:'cube_measure_form'},
        {title: 'Refresh Setting', src: 'partials/cubeDesigner/refresh_settings.html', isComplete: false,form:'refresh_setting_form'},
        {title: 'Advanced Setting', src: 'partials/cubeDesigner/advanced_settings.html', isComplete: false,form:'cube_setting_form'},
        {title: 'Configuration Overwrites ', src: 'partials/cubeDesigner/cubeOverwriteProp.html', isComplete: false,form:'cube_overwrite_prop_form'},
        {title: 'Overview', src: 'partials/cubeDesigner/overview.html', isComplete: false,form:null}
    ];

    $scope.curStep = $scope.wizardSteps[0];
    $scope.allCubeNames = [];

  $scope.getTypeVersion=function(typename){
    var searchResult=/\[v(\d+)\]/.exec(typename);
    if(searchResult&&searchResult.length){
      return searchResult.length&&searchResult[1]||1;
    }else{
      return 1;
    }
  }
  $scope.removeVersion=function(typename){
    if(typename){
      return typename.replace(/\[v\d+\]/g,"").replace(/\s+/g,'');
    }
    return "";
  }

  //init encoding list
  $scope.store = {
    supportedEncoding:[],
    encodingMaps:{}
  }
  TableModel.getColumnTypeEncodingMap().then(function(data){
    $scope.store.encodingMaps=data;
  });
  CubeService.getValidEncodings({}, function (encodings) {
    if(encodings){
      for(var i in encodings)
        if(VdmUtil.isNotExtraKey(encodings,i)){
          var value = i
          var name = value;
          var typeVersion=+encodings[i]||1;
          var suggest=false,selecttips='';
          if(/\d+/.test(""+typeVersion)&&typeVersion>=1){
            for(var s=1;s<=typeVersion;s++){
              if(s==typeVersion){
                suggest=true;
              }
              if(value=="int"){
                name = "int (deprecated)";
                suggest=false;
              }
              if(typeVersion>1){
                selecttips=" (v"+s;
                if(s==typeVersion){
                  selecttips+=",suggest"
                }
                selecttips+=')';
              }
              $scope.store.supportedEncoding.push({
                "name":name+selecttips,
                "value":value+"[v"+s+"]",
                "version":typeVersion,
                "baseValue":value,
                "suggest":suggest
              });
            }
          }
        }
    }
  },function(e){
    $scope.store.supportedEncoding = $scope.cubeConfig.encodings;
  })
  $scope.getEncodings =function (name){
    var filterName=name;
    var columnType= $scope.modelsManager.getColumnTypeByColumnName(filterName);
    var matchList=VdmUtil.getObjValFromLikeKey($scope.store.encodingMaps,columnType);
    var encodings =$scope.store.supportedEncoding,filterEncoding;
    if($scope.isEdit){
      var rowkey_columns=$scope.cubeMetaFrame.rowkey.rowkey_columns;
      if(rowkey_columns&&filterName){
        for(var s=0;s<rowkey_columns.length;s++){
          if(filterName==rowkey_columns[s].column){
            var version=rowkey_columns[s].encoding_version;
            var noLenEncoding=rowkey_columns[s].encoding.replace(/:\d+/,"");
            filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'value',noLenEncoding+(version?"[v"+version+"]":"[v1]"),'suggest',true)
            matchList.push(noLenEncoding);
            filterEncoding=VdmUtil.getObjectList(filterEncoding,'baseValue',matchList);
            break;
          }
        }
      }else{
        filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'suggest',true);
        filterEncoding=VdmUtil.getObjectList(filterEncoding,'baseValue',matchList)
      }
    }else{
      filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'suggest',true);
      filterEncoding=VdmUtil.getObjectList(filterEncoding,'baseValue',matchList)
    }
    return filterEncoding;
  }
    $scope.getTypeVersion=function(typename){
      var searchResult=/\[v(\d+)\]/.exec(typename);
      if(searchResult&&searchResult.length){
        return searchResult.length&&searchResult[1]||1;
      }else{
        return 1;
      }
    }
    $scope.removeVersion=function(typename){
      if(typename){
        return typename.replace(/\[v\d+\]/g,"");
      }
      return "";
    }
  // ~ init
    if (!$scope.state) {
        $scope.state = {mode: "view"};
    }

    $scope.$watch('cubeMetaFrame', function (newValue, oldValue) {
        if (($scope.state.mode === "edit") && ($scope.cubeMode == "addNewCube")) {
          $scope.getAllCubeNames();
        }

        if(!newValue){
            return;
        }

        if ($scope.cubeMode=="editExistCube"&&newValue && !newValue.project) {
            initProject();
        }

    });

    $scope.removeElement = function (arr, element) {
        var index = arr.indexOf(element);
        if (index > -1) {
            arr.splice(index, 1);
        }
    };

    $scope.getAllCubeNames = function () {
      if ($scope.allCubeNames.length > 0) {
        $scope.allCubeNames.splice(0, $scope.allCubeNames.length);
      }

      var queryParam = {offset: 0, limit: 65535};
      CubeService.list(queryParam, function (all_cubes) {
        for (var i = 0; i < all_cubes.length; i++) {
          $scope.allCubeNames.push(all_cubes[i].name.toUpperCase());
        }
      });
    };

    $scope.open = function ($event) {
        $event.preventDefault();
        $event.stopPropagation();

        $scope.opened = true;
    };

    $scope.preView = function () {
        var stepIndex = $scope.wizardSteps.indexOf($scope.curStep);
        if (stepIndex >= 1) {
            $scope.curStep.isComplete = false;
            $scope.curStep = $scope.wizardSteps[stepIndex - 1];
        }
    };

    $scope.nextView = function () {
        var stepIndex = $scope.wizardSteps.indexOf($scope.curStep);

        if (stepIndex < ($scope.wizardSteps.length - 1)) {
            $scope.curStep.isComplete = true;
            $scope.curStep = $scope.wizardSteps[stepIndex + 1];

            AuthenticationService.ping(function (data) {
                UserService.setCurUser(data);
            });
        }
    };

    $scope.goToStep = function(stepIndex){
        if($scope.cubeMode == "addNewCube"){
            if(stepIndex+1>=$scope.curStep.step){
                return;
            }
        }
        for(var i=0;i<$scope.wizardSteps.length;i++){
            if(i<=stepIndex){
                $scope.wizardSteps[i].isComplete = true;
            }else{
                $scope.wizardSteps[i].isComplete = false;
            }
        }
        if (stepIndex < ($scope.wizardSteps.length)) {
            $scope.curStep = $scope.wizardSteps[stepIndex];

            AuthenticationService.ping(function (data) {
                UserService.setCurUser(data);
            });
        }
    }

  $scope.$watch('cube.detail', function (newValue, oldValue) {
    if (!newValue) {
      return;
    }
    if (newValue && $scope.state.mode === "view") {
      $scope.cubeMetaFrame = newValue;

      // when viw state,each cubeSchema has its own metaModel
      $scope.metaModel = {
        model: {}
      }
      $scope.metaModel.model=modelsManager.getModel($scope.cubeMetaFrame.model_name);

    }
  });

  $scope.$watch('cubeMetaFrame', function (newValue, oldValue) {
    if (!newValue) {
      return;
    }
    if ($scope.cubeMode == "editExistCube" && newValue && !newValue.project) {
      initProject();
    }

  });
  $scope.nextStep = async function (stepIndex, cb) {
    var validResult = await $scope.checkCubeForm(stepIndex);
    if (validResult) {
      if (typeof cb === 'function') {
        cb(stepIndex)
      }
    }
  }
  $scope.checkCubeForm = async function(stepIndex){
    // do not check for Prev Step
    if (stepIndex + 1 < $scope.curStep.step) {
      return true;
    }

    if(!$scope.curStep.form){
          return true;
      }
      if($scope.state.mode==='view'){
          return true;
      }
      else{
          //form validation
          if($scope.forms[$scope.curStep.form].$invalid){
              $scope.forms[$scope.curStep.form].$submitted = true;
              return false;
          }else{
              //business rule check
              switch($scope.curStep.form){
                  case 'cube_info_form':
                    return await $scope.check_cube_info();
                  case 'cube_dimension_form':
                      return $scope.check_cube_dimension();
                  case 'cube_measure_form':
                      return $scope.check_cube_measure();
                  case 'cube_setting_form':
                      return $scope.check_cube_setting();
                  case 'cube_overwrite_prop_form':
                      return $scope.cube_overwrite_prop_check();
                  default:
                      return true;
              }
          }
      }
  };

  $scope.checkDuplicatedCubeName = function (cubeName) {
    return ($scope.allCubeNames.indexOf(cubeName.toUpperCase())) >= 0;
  }

  $scope.check_cube_info = function () {
    // Update storage type according to the streaming table in model
    if(TableModel.selectProjectTables.some(function(table) {
      return (table.name === $scope.metaModel.model.fact_table && _.values(tableConfig.streamingSourceType).indexOf(table.source_type) > -1)
    })) {
      $scope.cubeMetaFrame.storage_type = 3;
    } else {
      $scope.cubeMetaFrame.storage_type = 2;
    }
    var defer = $q.defer();
    if ($scope.state.mode === "edit" && $scope.cubeMode === "addNewCube") {
      var cubeName = $scope.cubeMetaFrame.name;
      CubeService.checkDuplicateCubeName({cubeId: cubeName}, {}, function (res) {
        if (!res.data) {
          SweetAlert.swal('Oops...', "The cube named [" + cubeName.toUpperCase() + "] already exists", 'warning');
        }
        return defer.resolve(res.data);
      })
    } else {
      defer.resolve(true);
    }
    return defer.promise;
  }

    $scope.check_cube_dimension = function(){
        var errors = [];
        if(!$scope.cubeMetaFrame.dimensions.length){
            errors.push("Dimension can't be null");
        }
        var errorInfo = "";
        angular.forEach(errors,function(item){
            errorInfo+="\n"+item;
        });
        if(errors.length){
            SweetAlert.swal('', errorInfo, 'warning');
            return false;
        }else{
            return true;
        }
    };

    $scope.check_cube_measure = function(){
        var _measures = $scope.cubeMetaFrame.measures;
        var errors = [];
        if(!_measures||!_measures.length){
            errors.push("Please define your metrics.");
        }

        var existCountExpression = false;
        for(var i=0;i<_measures.length;i++){
            if(_measures[i].function.expression=="COUNT"){
                existCountExpression=true;
                break;
            }
        }
        if(!existCountExpression){
            errors.push("[COUNT] metric is required.");
        }

        var errorInfo = "";
        angular.forEach(errors,function(item){
            errorInfo+="\n"+item;
        });
        if(errors.length){
            SweetAlert.swal('', errorInfo, 'warning');
            return false;
        }else{
            return true;
        }
    }

    $scope.check_cube_setting = function(){
        var errors = [];

        angular.forEach($scope.cubeMetaFrame.aggregation_groups,function(group){
            if(!group&&!group.includes){
                errors.push("Each aggregation group can't be empty.");
            }
        })

        var shardRowkeyList = [];
        angular.forEach($scope.cubeMetaFrame.rowkey.rowkey_columns,function(rowkey){
          if(rowkey.isShardBy == true){
            shardRowkeyList.push(rowkey.column);
          }
          if(rowkey.encoding.substr(0,3)=='int' && (rowkey.encoding.substr(4)<1 || rowkey.encoding.substr(4)>8)){
            errors.push("int encoding column length should between 1 and 8.");
          }
          if(rowkey.encoding.substr(0, 5) == 'fixed' && (!/^[1-9]\d*$/.test(rowkey.encoding.split(':')[1]))) {
            errors.push("fixed encoding need a valid length.")
          }
        })
        if(shardRowkeyList.length >1){
          errors.push("At most one 'shard by' column is allowed.");
        }

        var cfMeasures = [];
        angular.forEach($scope.cubeMetaFrame.hbase_mapping.column_family,function(cf){
          angular.forEach(cf.columns[0].measure_refs, function (measure, index) {
            cfMeasures.push(measure);
          });
        });

        var uniqCfMeasures = _.uniq(cfMeasures);
        if(uniqCfMeasures.length != $scope.cubeMetaFrame.measures.length) {
          errors.push("All measures need to be assigned to column family");
        }

        var isCFEmpty = _.some($scope.cubeMetaFrame.hbase_mapping.column_family, function(colFamily) {
          return colFamily.columns[0].measure_refs.length == 0;
        });

        if (isCFEmpty == true) {
          errors.push("Each column family can't not be empty");
        }


        angular.forEach($scope.cubeMetaFrame.measures, function (measure, index) {
            if (measure.function.expression === 'COUNT_DISTINCT' && measure.function.returntype === 'bitmap' && !$scope.isIntMeasure(measure)) {
                var measureColumn = measure.function.parameter.value;

                var isColumnExit = false;
                angular.forEach($scope.cubeMetaFrame.dictionaries, function (dictionaries) {
                    if (!isColumnExit) {
                        //keep backward compatibility
                        if (dictionaries.column == measureColumn || dictionaries.column == VdmUtil.removeNameSpace(measureColumn))
                            isColumnExit = true;
                    }
                });

                if (!isColumnExit) {
                    errors.push("The non-Int type precise count distinct measure must set advanced dictionary: " + measureColumn);
                }
            }
        });

        if ($scope.cubeMetaFrame.engine_type === 6) {
          $scope.cubeMetaFrame.storage_type = 4;
        }

        var errorInfo = "";
        angular.forEach(errors,function(item){
            errorInfo+="\n"+item;
        });
        if(errors.length){
            SweetAlert.swal('', errorInfo, 'warning');
            return false;
        }else{
            return true;
        }
    }

    $scope.cube_overwrite_prop_check = function(){
      var errors = [];

      for(var key in $scope.cubeMetaFrame.override_kylin_properties){
        if(key==''){
          errors.push("Property name is required.");
        }
        if($scope.cubeMetaFrame.override_kylin_properties[key] == ''){
          errors.push("Property value is required.");
        }
      }

      var errorInfo = "";
      angular.forEach(errors,function(item){
        errorInfo+="\n"+item;
      });
      if(errors.length){
        SweetAlert.swal('', errorInfo, 'warning');
        return false;
      }else{
        return true;
      }
    }

    // ~ private methods
    function initProject() {
        ProjectService.listReadable({}, function (projects) {
            $scope.projects = projects;

            var cubeName = (!!$scope.routeParams.cubeName)? $scope.routeParams.cubeName:$scope.state.cubeName;
            if (cubeName) {
                var projName = null;
                if(ProjectModel.getSelectedProject()){
                    projName=ProjectModel.getSelectedProject();
                }else{
                    angular.forEach($scope.projects, function (project, index) {
                        angular.forEach(project.realizations, function (unit, index) {
                            if (!projName && unit.type=="CUBE"&&unit.realization === cubeName) {
                                projName = project.name;
                            }
                        });
                    });
                }

                if(!ProjectModel.getSelectedProject()){
                    ProjectModel.setSelectedProject(projName);
                    TableModel.aceSrcTbLoaded();
                }

                $scope.cubeMetaFrame.project = projName;
            }

            angular.forEach($scope.projects, function (project, index) {
                $scope.listAccess(project, 'ProjectInstance');
            });
        });
    }
});
