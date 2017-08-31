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

KylinApp.service('CubeList',function(CubeService,$q,AccessService){
    var _this = this;
    this.cubes=[];

    this.list = function(queryParam){

        var defer = $q.defer();
        CubeService.list(queryParam, function (_cubes) {
            angular.forEach(_cubes, function (cube, index) {
                if(cube.name){
                    cube.last_build_time = undefined;
                    if (cube.segments && cube.segments.length > 0) {
                        for(var i= cube.segments.length-1;i>=0;i--){
                            if(cube.segments[i].status==="READY"){
                                if(cube.last_build_time===undefined || cube.last_build_time<cube.segments[i].last_build_time)
                                    cube.last_build_time = cube.segments[i].last_build_time;
                            }
                        }
                    }
                }
            });
            _cubes = _.filter(_cubes,function(cube){return cube.name!=undefined});
            _this.cubes = _this.cubes.concat(_cubes);
            defer.resolve(_cubes.length);
        },function(){
            defer.reject("Failed to load cubes");
        });
        return defer.promise;
    };

    this.removeCube = function(cube){
        var cubeIndex = _this.cubes.indexOf(cube);
        if (cubeIndex > -1) {
            _this.cubes.splice(cubeIndex, 1);
        }
    }

    this.removeAll = function(){
        _this.cubes=[];
    };

});
