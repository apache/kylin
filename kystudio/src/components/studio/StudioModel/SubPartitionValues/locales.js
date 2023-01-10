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
    subParValuesTitle: 'Subpartition Values',
    searchPlaceholder: 'Search by subpartition value',
    segmentAmount: 'Segment Amount',
    segmentAmountTips: 'Amount of the segments built with this subpartition value / Total amount',
    addSubParValueTitle: 'Add Subpartition Value',
    addSubParValueDesc: 'The added values could be selected when building indexes for subpartitions.',
    multiPartitionPlaceholder: 'Please import, use comma (,) to separate multiple values',
    deleteSubPartitionValuesTitle: 'Delete Subpartition Value',
    deleteSubPartitionValuesTip: '{subSegsLength} subpartition(s) are selected. The deleted values couldn\'t be selected when building indexes for subpartitions. <br>Are you sure you want to delete?',
    deleteSubPartitionValuesByBuild: '{subSegsLength} subpartition(s) are selected. The deleted values could\'t be selected when building indexes for subpartitions. <br>The following {subSegsByBuildLength} subpartition(s) have been built already. The built data would be deleted as well. <br>Are you sure you want to delete?',
    duplicatePartitionValueTip: 'Some values are duplicated.',
    removeDuplicateValue: 'Clear invalid values'
  }
}
