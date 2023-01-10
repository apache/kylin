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
export default {
  en: {
    noSupportRawTable: 'Only KAP PLUS Provides Raw Table',
    tableIndex: 'Table Index',
    ID: 'ID',
    column: 'Column',
    dataType: 'Data Type',
    tableAlias: 'Table Alias',
    Encoding: 'Encoding',
    Length: 'Length',
    Index: 'Index',
    sort: 'Order',
    ConfigRawTable: 'Config Table Index',
    sortBy: 'Sort By',
    shardBy: 'Shard By',
    checkRowkeyInt: 'Integer encoding column length should between 1 and 8.',
    fixedLengthTip:
      'The length parameter of Fixed Length encoding is required.',
    fixedLengthHexTip:
      'The length parameter of Fixed Length Hex encoding is required.',
    fuzzyTip: 'Fuzzy index can be applied to string(varchar) type column only.',
    rawtableSortedWidthDate:
      'The first \'sorted\' column should be a column with encoding \'integer\', \'time\' or \'date\'.',
    rawtableSetSorted:
      'Please set at least one column with Sort By value of \'true\'.',
    sortByNull: 'The \'sorted\' column should not be null',
    tableIndexDetail: 'Table Index Detail',
    today: 'Today',
    thisWeek: 'This Week',
    lastWeek: 'Last Week',
    longAgo: 'Long Ago',
    tableIndexName: 'Table Index Name:',
    tableIndexId: 'Table Index ID:',
    searchTip: 'Search Table Index ID',
    broken: 'Broken',
    available: 'Available',
    empty: 'Emtpy Index',
    manualAdvice: 'User-defined index',
    autoAdvice: 'System-defined index',
    delTableIndexTip: 'Are you sure you want to delete the table index {tableIndexName}?',
    delTableIndexTitle: 'Delete Table Index',
    buildIndex: 'Build Index'
  }
}
