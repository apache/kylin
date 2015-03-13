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

/**
 *MetaModel will manage model info of cube
 */
KylinApp.service('MetaModel',function(){

    var _this = this;
    //data model when edit model
    this.name=null;
    this.fact_table=null;
    this.lookups=[];
    this.filter_condition=null;
    this.capacity=null;
    this.dimensions=[];
    this.metrics=[];
    this.partition_desc = {
        "partition_date_column" : '',
        "partition_date_start" : 0,
        "partition_type" : 'APPEND'
    };
    this.last_modified=0;

    this.setMetaModel =function(model){
        _this.name = model.name;
        _this.fact_table = model.fact_table;
        _this.lookups =model.lookups;
        _this.filter_condition = model.filter_condition;
        _this.capacity = model.capacity;
        _this.partition_desc = model.partition_desc;
        _this.last_modified = model.last_modified;
        _this.metrics  = model.metrics;
        _this.dimensions = model.dimensions;
    };


    this.converDateToGMT = function(){
        if(this.partition_desc&&this.partition_desc.partition_date_start){
            this.partition_desc.partition_date_start+=new Date().getTimezoneOffset()*60000;
        }
    };
    //
    this.createNew = function () {
            var metaModel = {
                name: '',
                fact_table: '',
                lookups: [],
                filter_condition:'',
                capacity:'MEDIUM',
                dimensions:[],
                metrics:[],
                "partition_desc" : {
                    "partition_date_column" : '',
                    "partition_date_start" : 0,
                    "partition_type" : 'APPEND'
                },
                last_modified:0
            };

            return metaModel;
        }
})
