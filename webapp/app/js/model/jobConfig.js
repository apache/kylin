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

KylinApp.constant('jobConfig', {
  allStatus: [
    {name: 'NEW', value: 0, count: ''},
    {name: 'PENDING', value: 1, count: ''},
    {name: 'RUNNING', value: 2, count: ''},
    {name: 'STOPPED', value: 32, count: ''},
    {name: 'FINISHED', value: 4, count: ''},
    {name: 'ERROR', value: 8, count: ''},
    {name: 'DISCARDED', value: 16, count: ''}
  ],
  timeFilter: [
    {name: 'CURRENT DAY', value: 0},
    {name: 'LAST ONE DAY', value: 1},
    {name: 'LAST ONE WEEK', value: 2},
    {name: 'LAST ONE MONTH', value: 3},
    {name: 'LAST ONE YEAR', value: 4},
    {name: 'ALL', value: 5},
  ],
  theaditems: [
    {attr: 'name', name: 'Job Name'},
    {attr: 'related_cube', name: 'Cube'},
    {attr: 'progress', name: 'Progress'},
    {attr: 'last_modified', name: 'Last Modified Time'},
    {attr: 'duration', name: 'Duration'}
  ],
  searchMode: [
    {name: 'CUBING', value: 'CUBING_ONLY'},
    {name: 'CHECK POINT', value: 'CHECKPOINT_ONLY'},
    {name: 'CARDINALITY', value: 'CARDINALITY_ONLY'},
    {name: 'SNAPSHOT', value: 'SNAPSHOT_ONLY'},
    {name: 'ALL', value: 'ALL'}
  ],
  queryitems: [
  {attr: 'server', name: 'Server'},
  {attr: 'user', name: 'User'},
  {attr: 'cube', name: 'Hit Cube'},
  {attr: 'sql', name: 'Sql'},
  {attr: 'adj', name: 'Description'},
  {attr: 'running_seconds', name: 'Running Seconds'},
  {attr: 'start_time', name: 'Start Time'},
  {attr: 'last_modified', name: 'Last Modified'},
  {attr: 'thread', name: 'Thread'}
]

});
