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

// TODO add cubes manager
KylinApp.service('cubesManager', function (CubeDescService,SweetAlert) {

    this.currentCube = {};

    this.cubeMetaFrame={};

    this.getCubeDesc = function(_cubeName){
      CubeDescService.query({cube_name: _cubeName}, {}, function (detail) {
        if (detail.length > 0&&detail[0].hasOwnProperty("name")) {
          return detail[0];
        }else{
          SweetAlert.swal('Oops...', "No cube detail info loaded.", 'error');
        }
      }, function (e) {
        if(e.data&& e.data.exception){
          var message =e.data.exception;
          var msg = !!(message) ? message : 'Failed to take action.';
          SweetAlert.swal('Oops...', msg, 'error');
        }else{
          SweetAlert.swal('Oops...', "Failed to take action.", 'error');
        }
      });

  }

})
