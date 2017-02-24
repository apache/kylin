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

KylinApp.constant('cubeConfig', {

  //~ Define metadata & class
  measureParamType: ['column', 'constant'],
  measureExpressions: ['SUM', 'MIN', 'MAX', 'COUNT', 'COUNT_DISTINCT',"TOP_N", 'RAW','EXTENDED_COLUMN','PERCENTILE'],
  dimensionDataTypes: ["string", "tinyint", "int", "bigint", "date"],
  cubePartitionTypes: ['APPEND'],
  engineType:[
    {name:'MapReduce',value: 2},
    {name:'Spark (Beta)',value: 4}
  ],
  joinTypes: [
    {name: 'Left', value: 'left'},
    {name: 'Inner', value: 'inner'}
  ],
  queryPriorities: [
    {name: 'NORMAL', value: 50},
    {name: 'LOW', value: 70},
    {name: 'HIGH', value: 30}
  ],
  measureDataTypes: [
    {name: 'INT', value: 'int'},
    {name: 'BIGINT', value: 'bigint'},
    {name: 'DECIMAL', value: 'decimal'},
    {name: 'DOUBLE', value: 'double'},
    {name: 'DATE', value: 'date'},
    {name: 'STRING', value: 'string'}
  ],
  distinctDataTypes: [
    {name: 'Error Rate < 9.75%', value: 'hllc(10)'},
    {name: 'Error Rate < 4.88%', value: 'hllc(12)'},
    {name: 'Error Rate < 2.44%', value: 'hllc(14)'},
    {name: 'Error Rate < 1.72%', value: 'hllc(15)'},
    {name: 'Error Rate < 1.22%', value: 'hllc(16)'},
    {name: 'Precisely (More Memory And Storage Needed)', value: 'bitmap'}
  ],
  topNTypes: [
    {name: 'Top 10', value: "topn(10)"},
    {name: 'Top 100', value: "topn(100)"},
    {name: 'Top 500', value: "topn(500)"},
    {name: 'Top 1000', value: "topn(1000)"},
    {name: 'Top 5000', value: "topn(5000)"},
    {name: 'Top 10000', value: "topn(10000)"}
  ],
  dftSelections: {
    measureExpression: 'SUM',
    measureParamType: 'column',
    measureDataType: {name: 'BIGINT', value: 'bigint'},
    distinctDataType: {name: 'Error Rate < 4.88%', value: 'hllc12'},
    queryPriority: {name: 'NORMAL', value: 50},
    cubePartitionType: 'APPEND',
    topN:{name: 'Top 100', value: "topn(100)"}
  },
    dictionaries: ["true", "false"],
    encodings:[
      {name:"dict",value:"value"},
      {name:"fixed_length",value:"fixed_length"},
      {name:"int (deprecated)",value:"int"}
    ],
    intEncodingOptions: [1,2,3,4,5,6,7,8],
//    cubes config
  theaditems: [
    {attr: 'name', name: 'Name'},
    {attr: 'status', name: 'Status'},
    {attr: 'size_kb', name: 'Cube Size'},
    {attr: 'input_records_count', name: 'Source Records'},
    {attr: 'last_build_time', name: 'Last Build Time'},
    {attr: 'owner', name: 'Owner'},
    {attr: 'create_time_utc', name: 'Create Time'}
  ],
  streamingAutoGenerateMeasure:[
    {name:"year_start",type:"date"},
    {name:"quarter_start",type:"date"},
    {name:"month_start",type:"date"},
    {name:"week_start",type:"date"},
    {name:"day_start",type:"date"},
    {name:"hour_start",type:"timestamp"},
    {name:"minute_start",type:"timestamp"}
  ],
  partitionDateFormatOpt:[
    'yyyy-MM-dd',
    'yyyyMMdd',
    'yyyy-MM-dd HH:mm:ss'
  ],
  partitionTimeFormatOpt:[
    'HH:mm:ss',
    'HH:mm',
    'HH'
  ],
  rowKeyShardOptions:[
    true,false
  ],
  statusNeedNofity:['ERROR', 'DISCARDED', 'SUCCEED'],
  buildDictionaries:[
    {name:"Global Dictionary", value:"org.apache.kylin.dict.GlobalDictionaryBuilder"}
  ],
  needSetLengthEncodingList:['fixed_length','fixed_length_hex','int','integer']
});
