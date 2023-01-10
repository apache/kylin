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
    job_notification_states: data.job_notification_states
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

export const jobNotificationStateTypes = [
  'Succeed',
  'Error',
  'Discard'
]
