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
