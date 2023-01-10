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
    addSnapshotTitle: 'Add Snapshot',
    filterTableName: 'Search Database/Table',
    noSourceData: 'No data',
    refreshNow: 'Refresh now',
    refreshIng: 'Refreshing',
    selectedDBValidateFailText: 'Please enter database name',
    selectedTableValidateFailText: 'Please enter table name as \'database.table\'.',
    database: 'Database',
    tableName: 'Table',
    dbPlaceholder: 'Enter database name or select from the left',
    dbTablePlaceholder: 'Enter \'database.table\' or select from the left',
    refreshText: 'Can\'t find what you\'re looking for?',
    refreshTips: 'The system caches the source table metadata periodically. If you can\'t find what you\'re looking for, you can refresh immediately or wait for the system to finish refreshing.',
    selectAll: 'Select All',
    cleanAll: 'Clean All',
    synced: 'Added',
    sourceTablePartitionSetting: 'Partition Setting for Source Table',
    sourceTablePartitionTip: 'Snapshots could be built more efficiently with partitioning for the source table. Please note that the partition column needs to be identical to the first-level partition column of the source table.',
    pleaseFilterDBOrTable: 'Search by table or database name',
    table: 'Table Name',
    partitionColumn: 'Partition Column',
    partitionValue: 'Partition Value',
    selectPartitionPlaceholder: 'Please select partition column',
    noPartition: 'No Partition',
    detectPartition: 'Detect source table partition',
    fetchPartitionErrorTip: 'Can\'t detect the partition column now. Please set it manually.',
    viewAllPartition: 'View All',
    preStep: 'Previous',
    undefinedPartitionColErrorTip: 'This source table has no partition set yet.',
    emptyPartitionSettingTip: 'The snapshots of the selected tables have already been added. Please select again.',
    repairSnapshotTitle: 'Snapshot Repair',
    readyPartitions: 'Built',
    notReadyPartitions: 'Not built',
    noPartitionValuesError: 'Partition value can\'t be empty'
  }
}
