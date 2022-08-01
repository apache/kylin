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
    job_error_notification_enabled: data.job_error_notification_enabled,
    data_load_empty_notification_enabled: data.data_load_empty_notification_enabled,
    job_notification_emails: jobEmails
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

export function _getSnapshotSetting (data) {
  return {
    project: data.project,
    snapshot_manual_management_enabled: data.snapshot_manual_management_enabled
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
