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
  'en': {
    tableName: 'Table Name:',
    partitionKey: 'Time Partition:',
    loadInterval: 'Load Interval:',
    storageType: 'Storage Type:',
    storageSize: 'Storage Size:',
    totalRecords: 'Loaded Records:',
    rows: 'Rows',
    noPartition: 'No Partition',
    changePartitionTitle: 'Change Partition',
    changePartitionContent1: 'You\'re going to change the table {tableName}\'s partition from {oldPartitionKey} to {newPartitionKey}.',
    changePartitionContent2: 'The change may clean loaded storage {storageSize} and re-load data.',
    changePartitionContent3: 'Do you really need to change the partition?',
    fullLoadDataTitle: 'Load Data',
    fullLoadDataContent1: 'Because the table {tableName} has no partition, the load data job will reload all impacted data {storageSize}.',
    fullLoadDataContent2: 'Do you need to submit the load data job?',
    loadSuccessTip: 'Success to submit the load data job. Please view it on the monitor page.',
    loadRange: 'Loaded Range',
    loadData: 'Load Data',
    refreshData: 'Refresh Data',
    notLoadYet: 'Not loaded yet',
    suggestSetLoadRangeTip: 'Please enter a load range after the partition set, or the system wouldn\'t accelerate queries automatically.',
    dateFormat: 'Time Format:',
    detectFormat: 'Detect partition format'
  }
}
