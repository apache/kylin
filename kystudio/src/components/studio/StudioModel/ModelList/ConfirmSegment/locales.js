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
    storageSize: 'Storage Size',
    modifyTime: 'Last Updated Time',
    sourceRecords: 'Source Records',
    cannotBuildTips: 'Only ONLINE or WARNING segments could be refreshed.',
    failedSegmentsTips: 'Successfully submitted {sucNum} jobs. Can\'t submit {failNum} jobs.',
    details: 'The following segments can\'t be built as they might not exist or be locked at the moment. Please check and try again. ',
    failedTitle: 'Can\'t Submit Jobs',
    gotIt: 'Got It',
    noSegmentList: 'Not data range could be selected at the moment. There might be no segment existed, or all indexes might have been built to all segments.',
    parallelBuild: 'Build multiple segments in parallel',
    parallelBuildTip: 'By default, only one build job would be generated for all segments. With this option checked, multiple jobs would be generated and segments would be built in parallel.',
    subPratitionAmount: 'Subpartition Amount',
    subPratitionAmountTip: 'Amount of the built ones / Total amount'
  }
}
