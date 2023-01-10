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
import { transToUTCMs } from '../../../util'

export const editTypes = {
  LOAD_DATA: 'loadData',
  REFRESH_DATA: 'refreshData'
}
export const fieldTypes = {
  IS_LOAD_EXISTED: 'isLoadExisted',
  LOAD_DATA_RANGE: 'loadDataRange',
  REFRESH_DATA_RANGE: 'freshDataRange'
}
export const fieldVisiableMaps = {
  [editTypes.LOAD_DATA]: [ fieldTypes.IS_LOAD_EXISTED, fieldTypes.LOAD_DATA_RANGE ],
  [editTypes.REFRESH_DATA]: [ fieldTypes.REFRESH_DATA_RANGE ]
}
export const titleMaps = {
  [editTypes.LOAD_DATA]: 'loadData',
  [editTypes.REFRESH_DATA]: 'refreshData'
}
export const validate = {
  [fieldTypes.LOAD_DATA_RANGE] (rule, value, callback) {
    const [ startValue, endValue ] = value
    if ((!startValue || !endValue || transToUTCMs(startValue) >= transToUTCMs(endValue))) {
      callback(new Error(this.$t('invaildDate')))
    } else {
      callback()
    }
  },
  [fieldTypes.REFRESH_DATA_RANGE] (rule, value, callback) {
    const [ startValue, endValue ] = value
    if ((!startValue || !endValue || transToUTCMs(startValue) >= transToUTCMs(endValue))) {
      callback(new Error(this.$t('invaildDate')))
    } else {
      callback()
    }
  }
}

export function _getLoadDataForm (that) {
  const { form, project, table } = that
  const { isLoadExisted, loadDataRange } = form
  return {
    project: project.name,
    table: `${table.database}.${table.name}`,
    // isLoadExisted: isLoadExisted,
    start: !isLoadExisted ? String(transToUTCMs(loadDataRange[0])) : undefined,
    end: !isLoadExisted ? String(transToUTCMs(loadDataRange[1])) : undefined
  }
}
export function _getRefreshDataForm (that) {
  const { form, project, table } = that
  return {
    projectName: project.name,
    tableFullName: `${table.database}.${table.name}`,
    startTime: transToUTCMs(form.freshDataRange[0]),
    endTime: transToUTCMs(form.freshDataRange[1]),
    affected_start: '',
    affected_end: ''
  }
}

export function _getNewestTableRange (project, table) {
  return {
    projectName: project.name,
    tableFullName: `${table.database}.${table.name}`
  }
}
