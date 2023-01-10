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
    overview: 'Overview',
    segment: 'Segment',
    indexes: 'Index',
    indexOverview: 'Index Overview',
    aggregateGroup: 'Aggregate Group',
    tableIndex: 'Table Index',
    build: 'Build Index',
    developers: 'Developers',
    dataFeatures: 'Data Features',
    segmentHoletips: 'There exists a gap in the segment range, and the data of this range cannot be queried. Please confirm whether to add the following segments to fix.',
    fixSegmentTitle: 'Fix Segment',
    modelMetadataChangedDesc: 'Source table in the following segment(s) might have been changed. The data might be inconsistent after being built. Please check with your system admin.<br/>You may try refreshing these segments to ensure the data consistency.',
    buildIndex: 'Build Index',
    batchBuildSubTitle: 'Please choose which data ranges you\'d like to build with the added indexes.',
    hybridModelBuildTitle: 'Please choose which data ranges you\'d like to build with the added batch index(es). (Streaming index(es) will be built when streaming job is started again.)',
    modelEditAction: 'Edit',
    modelBuildAction: 'Build',
    moreAction: 'More',
    noData: 'No Data',
    recommendationsBtn: 'Recommendations',
    streaming: 'Streaming',
    noResult: 'No Result'
  }
}
