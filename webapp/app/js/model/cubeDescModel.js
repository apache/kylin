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

KylinApp.service('CubeDescModel', function (kylinConfig) {

  this.cubeMetaFrame = {};

  //
  this.createNew = function (defaultPara) {
    var cubeMeta = {
      "name": "",
      "model_name": "",
      "description": "",
      "dimensions": [],
      "measures": [
        {
          "name": "_COUNT_",
          "function": {
            "expression": "COUNT",
            "returntype": "bigint",
            "parameter": {
              "type": "constant",
              "value": "1",
              "next_parameter":null
            },
            "configuration":{}
          }
        }
      ],
      "dictionaries" :[],
      "rowkey": {
        "rowkey_columns": []
      },
      "aggregation_groups": []
      ,
      "partition_date_start":0,
      "partition_date_end":undefined,
      "notify_list": [],
      "hbase_mapping": {
        "column_family": []
      },
      "retention_range": "0",
      "status_need_notify":['ERROR', 'DISCARDED', 'SUCCEED'],
      "auto_merge_time_ranges": [604800000, 2419200000],
      "engine_type": kylinConfig.getCubeEng(),
      "storage_type":kylinConfig.getStorageEng(),
      "override_kylin_properties":{}
    };

    return angular.extend(cubeMeta,defaultPara) ;
  };

  this.createMeasure = function () {
    var measure = {
      "name": "",
      "function": {
        "expression": "",
        "returntype": "",
        "parameter": {
          "type": "",
          "value": "",
          "next_parameter":null
        },
        "configuration":null
      }
    };

    return measure;
  }
  this.createDictionaries = function () {
    var dictionaries = {
      "column": null,
      "builder": null,
      "reuse":null
    }
    return dictionaries;
  }

  this.createAggGroup = function () {
    var group = {
      "includes" : [],
      "select_rule" : {
        "hierarchy_dims" : [],//2 dim array
          "mandatory_dims" : [],
          "joint_dims" : [  ]//2 dim array
      }
    }

    return group;
  }



})
