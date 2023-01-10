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
    dialogTitle: 'Diagnosis',
    jobDiagnosis: 'Download Job Diagnostic Package',
    systemDiagnosis: 'Download System Diagnostic Package',
    generateBtn: 'Generate and Download',
    timeRange: 'Select Time Range',
    timeRangeTip: 'The selected time range should include the time when the failure occurred, based on the system time of the server node.',
    lastHour: 'Last 1 Hour',
    lastDay: 'Last 1 Day',
    lastThreeDay: 'Last 3 Days',
    lastMonth: 'Last 1 Month',
    custom: 'Customize',
    customTimeTip: 'The time range could vary from 5 minutes to 1 month.',
    selectDatePlaceholder: 'Select Date',
    server: 'Server',
    downloadTip: 'The diagnostic package would be downloaded automatically once it\'s successfully generated.',
    requireOverTime1: 'Generate timeout. Please ',
    requireOverTime2: ' or refer to the ',
    requireOverTime3: ' to check diagnosis tools.',
    manual: 'user manual',
    noAuthorityTip: 'Don\'t have permission to download the diagnostic package.',
    manualDownloadTip: 'If the automatic download fails, please check your network connectivity and browser security settings, then click ',
    manualDownload: 'Download Manually',
    download: 'Download',
    cancel: 'Cancel',
    details: 'Details',
    retry: 'retry',
    selectAll: 'Select All',
    otherErrorMsg: 'An error occurred when downloading the diagnostic package.',
    closeModelTip: 'You can go to $KYLIN-HOME/diag_dump/ to see the currently generated diagnostic packages.',
    closeModalTitle: 'The diagnostic package will be lost when you continue, do you want to continue?',
    modelTitle: 'Notice',
    confrimBtn: 'Discard',
    cancelBtn: 'Cancel',
    timeErrorMsg: 'The time range must be greater than or equal to 5 minutes and less than or equal  to 1 month. Please reselect.',
    selectServerTip: 'Please select the server(s)',
    selectServerPlaceHolder: 'Please select server',
    downloadJobDiagPackage1: 'The diagnostic package includes this job’s logs for the executor, driver and project metadata. For other error like query error, please download system diagnostic package in ',
    adminMode: 'admin mode',
    downloadJobDiagPackage2: '.',
    downloadJobDiagPackageForNorAdmin: 'The diagnostic package includes this job’s logs for Executor, Driver and project metadata. For other error like query error, please contact your system admin to download system diagnostic package.',
    downloadSystemDiagPackage1: 'For errors excluding job errors, please download system diagnostic package. It includes the system\'s metadata and logs.<br/>For job errors, please go to the ',
    jobPage: 'job page',
    downloadSystemDiagPackage2: ' to download the respective job diagnostic package.',
    downloadJobDiagPackage1ForIframe: 'The diagnostic package includes this job’s logs for the executor, driver and project metadata. For other error like query error or cluster scaling failure, please download system diagnostic package in ',
    workspaceList: 'workspace list',
    downloadJobDiagPackage2ForIframe: '.',
    downloadJobDiagPackageForNorAdminForIframe: 'The diagnostic package includes this job’s logs for Executor, Driver and project metadata. For other error like query error or cluster scaling failure, please contact your system admin to download system diagnostic package.',
    downloadJobDiagPackage3: 'The job is running, it may cause the diagnostic package to be incomplete. It’s recommended to discard the job first.',
    monitor: 'monitor',
    queryDiagnostic: 'Download Query Diagnostic Package',
    downloadQueryDiagnostic: 'The diagnostic package includes this query’s logs for the executor, driver and project metadata, it may not be complete due to the system limitation. Job error, please download the job diagnostic package in ',
    downloadQueryDiagnosticForKylinsubText1: '. For other error, please download system diagnostic package in ',
    downloadQueryDiagnosticForKylinsubText2: '.',
    downloadQueryDiagnosticForKylinsubText3: '. For other error, please contact your system admin to download system diagnostic package.',
    downloadQueryDiagnosticForKCsubText1: '. For other error like cluster scaling failure, please download system diagnostic package in ',
    deleteDiagnosticSuccess: 'Diagnostic package discarded',
    createDiagnostic: 'Generate diagnostic packages',
    queryPage: 'query history',
    downloadJobDiagnostic: 'The diagnostic package includes this job’s logs for the executor, driver and project metadata, it may not be complete due to the system limitation. Query error, please download the query diagnostic package in ',
    downloadJobDiagnosticSubText1: '. For other error, please download system diagnostic package in ',
    downloadJobDiagnosticSubText2: '. For other error, please contact your system admin to download system diagnostic package.',
    downloadJobDiagnosticSubText3: 'The diagnostic package includes this job’s logs for executor, driver and project metadata, it  may not be complete due to the system limitation. ',
    downloadJobDiagnosticSubText4: 'For other error like cluster scaling failure, please download system diagnostic package in ',
    downloadJobDiagnosticSubText5: 'For other error like cluster scaling failure, please contact your system admin to download system diagnostic package.'
  }
}
