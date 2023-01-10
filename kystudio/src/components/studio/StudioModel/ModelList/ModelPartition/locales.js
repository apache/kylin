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
    partitionSet: 'Partition Setting',
    modelPartitionSet: 'Model Partition',
    partitionDateTable: 'Partition Table',
    partitionDateColumn: 'Time Partition Column',
    multilevelPartition: 'Subpartition Column',
    multilevelPartitionDesc: 'A column from the selected table could be chosen. The models under this project could be partitioned by this column in addition to time partitioning. ',
    segmentChangedTips: 'With partition setting changed, all segments and data would be deleted. The model couldn’t serve queries. Meanwhile, the related ongoing jobs for building index would be discarded.',
    noPartition: 'No Partition',
    dateFormat: 'Time Format',
    detectFormat: 'Detect partition time format',
    noColumnFund: 'Column not found',
    pleaseInputColumn: 'Please select or enter a customized time format',
    changeSegmentTip1: 'You have modified the partition column as {tableColumn}, time format {dateType}. After saving, all segments under the model {modelName} will be purged. You need to reload the data, the model cannot serve related queries during data loading. Please confirm whether to submit?',
    changeSegmentTip2: 'You have modified as no partition column. After saving, all segments under the model {modelName} will be purged . The system will automatically rebuild the index and full load the data. The model cannot serve related queries during index building. Please confirm whether to submit?',
    changeSegmentTips: 'With partition setting changed, all segments and data would be deleted. The model couldn’t serve queries. Meanwhile, the related ongoing jobs for building index would be discarded.<br/>Do you want to continue?',
    previewFormat: 'Format preview: ',
    formatRule: 'The customized time format is supported. ',
    viewDetail: 'More info',
    rule1: 'Support using some elements of yyyy, MM, dd, HH, mm, ss, SSS in positive order',
    rule2: 'Support using - (hyphen), / (slash), : (colon), English space as separator',
    rule3: 'When using unformatted letters, use a pair of \' (single quotes) to quote, i.e. \'T\' will be recognized as T',
    secondStoragePartitionTips: 'Can\'t save the model partition. When the model uses incremental load method and the tiered storage is ON, the time partition column must be added as a dimension.'
  }
}
