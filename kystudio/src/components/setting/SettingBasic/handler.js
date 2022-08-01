export const projectTypeIcons = {
  MANUAL_MAINTAIN: 'el-icon-ksd-expert_mode_small'
}
export const lowUsageStorageTypes = [
  'DAY',
  'WEEK',
  'MONTH'
]
export const autoMergeTypes = [
  'HOUR',
  'DAY',
  'WEEK',
  'MONTH',
  'QUARTER',
  'YEAR'
]
export const volatileTypes = [
  'HOUR',
  'DAY',
  'WEEK',
  'MONTH',
  'QUARTER',
  'YEAR'
]
export const initialFormValue = {
  push_down_enabled: true,
  push_down_range_limited: true,
  auto_merge_enabled: true,
  auto_merge_time_ranges: [ 'WEEK', 'MONTH' ],
  storage_garbage: true,
  storage_quota_size: 0,
  storage_quota_tb_size: 0,
  volatile_range: {
    volatile_range_number: 0,
    volatile_range_enabled: false,
    volatile_range_type: 'DAY'
  },
  retention_range: {
    retention_range_number: 0,
    retention_range_enabled: false,
    retention_range_type: 'DAY'
  },
  alias: '',
  project: '',
  description: '',
  maintain_model_type: '',
  jdbc_datasource_enabled: false,
  JDBCConnectSetting: []
}
export const validate = {
  'positiveNumber' (rule, value, callback) {
    const regex = /^\d+(\.\d{1,2})?$/
    if (value === '' || value === undefined || value < 0 || isNaN(value) || !regex.test(value)) {
      callback(new Error(this.$t('emptyTips')))
    } else {
      callback()
    }
  },
  'storageQuotaSize' (rule, value, callback) {
    if (value === '' || value === undefined || value < 1 || isNaN(value)) {
      callback(new Error(this.$t('emptyTips')))
    } else {
      callback()
    }
  },
  'storageQuotaNum' (rule, value, callback) {
    if (value === '' || value === undefined || value < 0 || isNaN(value)) {
      callback(new Error(this.$t('emptyTips')))
    } else {
      callback()
    }
  }
}
export function _getProjectGeneralInfo (data) {
  let params = {
    project: data.project,
    alias: data.alias || data.project,
    description: data.description,
    maintain_model_type: data.maintain_model_type
  }
  // 专家档时才加 semi_automatic_mode 这个属性
  if (data.maintain_model_type === 'MANUAL_MAINTAIN') {
    params.semi_automatic_mode = data.semi_automatic_mode || false
  }
  return params
}
export function _getSegmentSettings (data, project) {
  return {
    project: data.project,
    auto_merge_time_ranges: data.auto_merge_time_ranges,
    auto_merge_enabled: data.auto_merge_enabled,
    volatile_range: {
      ...data.volatile_range,
      volatile_range_number: data.volatile_range.volatile_range_number + '',
      volatile_range_enabled: data.auto_merge_enabled
    },
    retention_range: {
      ...data.retention_range,
      retention_range_number: data.retention_range.retention_range_number + '',
      retention_range_enabled: data.retention_range.retention_range_enabled,
      retention_range_type: data.retention_range.retention_range_type
    },
    create_empty_segment_enabled: data.create_empty_segment_enabled
  }
}
export function _getPushdownConfig (data) {
  return {
    project: data.project,
    push_down_enabled: data.push_down_enabled,
    push_down_range_limited: data.push_down_range_limited
  }
}
export function _getStorageQuota (data) {
  return {
    project: data.project,
    storage_quota_size: data.storage_quota_size,
    storage_quota_tb_size: data.storage_quota_tb_size
  }
}
export function _getIndexOptimization (data) {
  return {
    project: data.project,
    low_frequency_threshold: data.low_frequency_threshold,
    frequency_time_window: data.frequency_time_window
  }
}

export function _getRetentionRangeScale (form) {
  let largestIdx = -1
  form.auto_merge_time_ranges.forEach(option => {
    const currentIdx = autoMergeTypes.indexOf(option)
    if (currentIdx > largestIdx) {
      largestIdx = currentIdx
    }
  })
  return autoMergeTypes[largestIdx]
}
