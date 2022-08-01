export const conflictTypes = {
  DUPLICATE_MODEL_NAME: 'DUPLICATE_MODEL_NAME',
  TABLE_NOT_EXISTED: 'TABLE_NOT_EXISTED',
  COLUMN_NOT_EXISTED: 'COLUMN_NOT_EXISTED',
  INVALID_COLUMN_DATATYPE: 'INVALID_COLUMN_DATATYPE'
}

export const conflictOrder = Object.keys(conflictTypes)

export const importableConflictTypes = [
]

export const brokenConflictTypes = [
  conflictTypes.DUPLICATE_MODEL_NAME,
  conflictTypes.TABLE_NOT_EXISTED,
  conflictTypes.COLUMN_NOT_EXISTED,
  conflictTypes.INVALID_COLUMN_DATATYPE
]

export function formatConflictsGroupByName (conflicts = []) {
  const conflictsGroupsMap = {}

  for (const conflict of conflicts) {
    if (conflictsGroupsMap[conflict.type]) {
      conflictsGroupsMap[conflict.type].push(conflict.items)
    } else {
      conflictsGroupsMap[conflict.type] = [conflict.items]
    }
  }

  const result = Object.entries(conflictsGroupsMap)
    .sort(([typeA], [typeB]) => conflictOrder.indexOf(typeA) < conflictOrder.indexOf(typeB) ? -1 : 1)
    .map(([type, conflictArray]) => ({ type, conflicts: conflictArray }))
  return result
}

export const validator = {
  file (that) {
    return (rule, value = null, callback) => {
      if (!value) {
        callback(new Error(that.$t('pleaseSelectFile')))
      } else if (value.size >= 5 * 1024 * 1024) {
        callback(new Error(that.$t('filesSizeError')))
      } else {
        callback()
      }
    }
  }
}

export function diffOverWriteModel (obj) {
  let arr = [];
  ['add', 'reduce', 'modified'].forEach(item => {
    if (obj[item]) {
      if (obj[item].tables && obj[item].tables.list.length && obj[item].tables.list.filter(it => it.type === 'MODEL_FACT').length && item !== 'modified') {
        arr.push('modelFactDiff')
      }
      if (obj[item].tables && obj[item].tables.list.length && obj[item].tables.list.filter(it => it.type !== 'MODEL_FACT').length && item !== 'modified') {
        arr.push('modelLockupDiff')
      }
      if (obj[item].modelFilter && obj[item].modelFilter.list.length) {
        arr.push('modelFilterDiff')
      }
      if (obj[item].modelJoin && obj[item].modelJoin.list.length) {
        arr.push('modelJoinDiff')
      }
      if (obj[item].partitionColumns && obj[item].partitionColumns.list.length) {
        arr.push('partitionColumnsDiff')
      }
    }
  })
  arr = [...new Set(arr)]
  return arr
}
