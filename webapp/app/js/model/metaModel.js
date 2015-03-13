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

    //data model when edit model
    this.model={
        name: null,
        fact_table: null,
        lookups: [],
        filter_condition:null,
        capacity:null,
        "partition_desc" : {
            "partition_date_column" : '',
            "partition_date_start" : 0,
            "partition_type" : 'APPEND'
        },
        last_modified:0
    };

    this.setMetaModel =function(model){
        var _model = {};
        _model.name = model.name;
        _model.fact_table = model.fact_table;
        _model.lookups =model.lookups;
        _model.filter_condition = model.filter_condition;
        _model.capacity = model.capacity;
        _model.partition_desc = model.partition_desc;
        _model.last_modified = model.last_modified;
        this.model = _model;
    };

    this.initModel = function(){
        this.model = this.createNew();
    }

    this.getMetaModel = function(){
        return this.model;
    };

    this.setFactTable = function(fact_table) {
        this.model.fact_table =fact_table;
    };


    this.converDateToGMT = function(){
        if(this.model.partition_desc&&this.model.partition_desc.partition_date_start){
            this.model.partition_desc.partition_date_start+=new Date().getTimezoneOffset()*60000;
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
