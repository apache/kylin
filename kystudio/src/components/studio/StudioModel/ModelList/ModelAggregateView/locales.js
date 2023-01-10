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
    viewAll: 'View all',
    custom: 'Custom',
    recommended: 'Recommended',
    aggregateGroupTitle: 'Aggregate-Group-{id}',
    numTitle: 'Index Amount: {num}',
    maxDimCom: 'Max Dimension Combination',
    dimComTips: 'The MDC set here applies only to this aggregation group',
    noLimitation: 'No Limitation',
    dimension: 'Dimension（{size}/{total}）',
    measure: 'Measure（{size}/{total}）',
    include: 'Include',
    mandatory: 'Mandatory',
    hierarchy: 'Hierarchy',
    joint: 'Joint',
    group: 'Group',
    includeMeasure: 'Include Measure',
    aggGroupTips: 'No aggregate group was defined. Please add dimensions and measures which would be used in aggregate queries into groups.',
    aggGroup: 'Aggregate Group',
    delAggregateTip: 'Are you sure you want to delete aggregate group "{aggId}"?',
    delAggregateTitle: 'Delete Aggregate Group',
    addAggGroup: 'Add Aggregate Group',
    aggAdvanced: 'Advanced Setting',
    colon: ': ',
    maxDimComTips: 'MDC (Max Dimension Combination) is the maximum amount of dimensions in the aggregate index. Please set this number carefully according to your query requirements. The MDC set here applies to all aggregate groups without separate MDC.',
    cardinalityMultiple: 'The Product of Cardinality: ',
    showAll: 'Show More',
    indexTimeRange: 'Index’s Time Range',
    refuseAddIndexTip: 'Can\'t add streaming indexes. Please stop the streaming job and then delete all the streaming segments.',
    refuseRemoveIndexTip: 'Can\'t delete streaming indexes. Please stop the streaming job and then delete all the streaming segments.',
    refuseEditIndexTip: 'Can\'t edit streaming indexes. Please stop the streaming job and then delete all the streaming segments.'
  }
}
