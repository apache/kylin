import { objectArraySort } from 'util'
export const validate = {
  'positiveNumber' (rule, value, callback) {
    if (value === '' || value === undefined || value <= 0) {
      callback(new Error(null))
    } else {
      callback()
    }
  },
  'validateYarnName' (rule, value, callback) {
    if (!value) {
      callback(rule.message[0])
    } else if (rule.type !== 'string') {
      callback(rule.message[1])
    } else {
      callback()
    }
  },
  'principalName' (rule, value, callback) {
    if (value === '' || value === undefined || value === null) {
      callback(new Error(null))
    } else {
      callback()
    }
  }
}

export function _getJobAlertSettings (data, isArrayDefaultValue, isSort) {
  let jobEmails = [...data.job_notification_emails]

  if (isArrayDefaultValue) {
    !jobEmails.length && jobEmails.push('')
  }

  isSort && jobEmails.sort()

  return {
    project: data.project,
    metadata_persist_notification_enabled: data.metadata_persist_notification_enabled,
    data_load_empty_notification_enabled: data.data_load_empty_notification_enabled,
    job_notification_emails: jobEmails,
    job_states_notification: data.job_states_notification
  }
}

export function _getDefaultDBSettings (data) {
  return {
    project: data.project,
    defaultDatabase: data.default_database
  }
}

export function _getYarnNameSetting (data) {
  return {
    project: data.project,
    yarn_queue: data.yarn_queue
  }
}

export function _getSecStorageSetting (data) {
  return {
    project: data.project,
    second_storage_enabled: data.second_storage_enabled,
    second_storage_nodes: data.second_storage_nodes ? objectArraySort(data.second_storage_nodes, true, 'name') : []
  }
}

export const frequencyTypes = [
  'DAY',
  'HOURS',
  'MINUTE'
]

export function _getSnapshotSetting (data) {
  return {
    project: data.project,
    snapshot_manual_management_enabled: data.snapshot_manual_management_enabled,
    snapshot_automatic_refresh_enabled: data.snapshot_automatic_refresh_enabled,
    snapshot_automatic_refresh_time_mode: data.snapshot_automatic_refresh_time_mode,
    snapshot_automatic_refresh_time_interval: data.snapshot_automatic_refresh_time_interval,
    snapshot_automatic_refresh_trigger_hours: data.snapshot_automatic_refresh_trigger_hours,
    snapshot_automatic_refresh_trigger_minute: data.snapshot_automatic_refresh_trigger_minute,
    snapshot_automatic_refresh_trigger_second: data.snapshot_automatic_refresh_trigger_second
  }
}

export function _getExposeCCSetting (data) {
  return {
    project: data.project,
    expose_computed_column: data.expose_computed_column
  }
}

export function _getKerberosSettings (data) {
  return {
    project: data.project,
    principal: data.principal
  }
}

export const jobNotificationStateTypes = ['succeed', 'error', 'discarded']
