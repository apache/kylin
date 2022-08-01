import Vue from 'vue'

export function getStepLineName (that, name) {
  const stepMap = {
    'Detect Resource': that.$t('detectResource'),
    'Load Data To Index': that.$t('loadDataToIndex'),
    'Merge Segment Data': that.$t('mergeSegmentData'),
    'Clean Up Old Segment': that.$t('cleanUpOldSegment'),
    'Update Metadata': that.$t('updateMetadata'),
    'Table Sampling': that.$t('tableSampling'),
    'Build Snapshot': that.$t('buildSnapshot'),
    'Clean Up Intermediate Table': that.$t('clearUpIntermediateTable')
  }
  return stepMap[name]
}

// 子步骤名称
export function getSubTasksName (that, name) {
  const subTaskNameMap = {
    'Waiting for yarn resources': that.$t('waitingYarnResource'),
    'Build or refresh snapshot': that.$t('buildOrRefreshSnapshot'),
    'Materialize fact table view': that.$t('materializeFactTableView'),
    'Generate global dictionary': that.$t('generateGlobalDict'),
    'Generate flat table': that.$t('generateFlatTable'),
    'Save flat table': that.$t('saveFlatTable'),
    'Get flat table statistics': that.$t('getFlatTableStatistics'),
    'Generate global dictionary of computed columns': that.$t('generateDictOfCC'),
    'Merge flat table': that.$t('mergeFlatTable'),
    'Merge indexes': that.$t('mergeIndexes'),
    'Merge flat table statistics': that.$t('mergeFlatTableStatistics'),
    'Sample Table Data': that.$t('sampleTableData'),
    'Build Snapshot': that.$t('buildSnapshot'),
    'Build indexes by layer': that.$t('buildIndexesByLayer'),
    'Update flat table statistics': that.$t('updateFlatTableStatistics')
  }
  return subTaskNameMap[name]
}

export function getSubTaskStatus (subTask) {
  const statusType = {
    'FINISHED': 'sub-tasks-status is-finished',
    'RUNNING': 'running',
    'PENDING': 'sub-tasks-status is-pending',
    'ERROR': 'sub-tasks-status is-error',
    'ERROR_STOP': 'sub-tasks-status is-error-stop',
    'DISCARDED': 'icons el-ksd-icon-discarded_6',
    'STOPPED': 'sub-tasks-status is-stop',
    'SKIP': 'icons el-ksd-icon-skip_6'
  }
  return statusType[subTask.step_status]
}

export function getStepStatusTips (that, status) {
  const statusTips = {
    'FINISHED': that.$t('step_finished'),
    'RUNNING': that.$t('step_running'),
    'PENDING': that.$t('step_pending'),
    'ERROR': that.$t('step_error'),
    'ERROR_STOP': that.$t('step_error_stop'),
    'DISCARDED': that.$t('step_discarded'),
    'STOPPED': that.$t('step_stopped'),
    'SKIP': that.$t('step_skip')
  }
  return statusTips[status]
}

// 格式化时间 Xh Xm
export function formatTime (time) {
  if (time < 0.01 * 60 * 1000) {
    return '< 0.01m'
  } else {
    const hour = Math.floor(time / 1000 / 60 / 60)
    const minutes = Vue.filter('number')((time - hour * 60 * 60 * 1000) / 1000 / 60, 2)
    return hour > 0 ? `${hour}h ${minutes}m` : `${minutes}m`
  }
}
