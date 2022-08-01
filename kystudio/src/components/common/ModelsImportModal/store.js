import * as actionTypes from '../../../store/types'
import api from '../../../service/api'
import { importableConflictTypes, brokenConflictTypes, conflictTypes } from './handler'
import { handleError, handleSuccessAsync } from '../../../util'

export function getInitialState () {
  return {
    isShow: false,
    callback: null,
    project: null,
    models: [],
    conflicts: [],
    form: {
      file: null,
      request: {}
    }
  }
}

function findConflictModelInZip (conflictItems, modelNamesMap) {
  let conflictModel = null
  for (const conflictItem of conflictItems) {
    const [modelName] = conflictItem.element.split('-')
    if (modelName in modelNamesMap) {
      conflictModel = modelName
    }
  }
  return conflictModel
}

// function formatParsedResponse (response) {
//   return {
//     models: response.models.map(modelResponse => ({
//       id: modelResponse.uuid,
//       name: modelResponse.name,
//       nodeType: 'model',
//       children: modelResponse.tables.map(tableResponse => ({
//         id: `${modelResponse.uuid}-${tableResponse.name}`,
//         name: tableResponse.name,
//         nodeType: 'table',
//         type: tableResponse.kind
//       }))
//     })),
//     conflicts: response.conflicts,
//     signature: response.signature
//   }
// }

function formatUploadData (form) {
  const uploadData = new FormData()
  uploadData.append('file', form.file)
  return uploadData
}

function formatImportData (form) {
  const importData = new FormData()
  importData.append('file', form.file)
  // importData.append('models', form.models)
  importData.append('request', new window.Blob([JSON.stringify(form.request)], {type: 'application/json'}))
  // form.models.forEach((it, index) => {
  //   importData.append(`models[${index}]`, it)
  // })
  return importData
}

export function getDefaultAction (v) {
  if (!v) return
  let defaultValue = ''
  let disabledValue = []
  if (!v.importable) {
    defaultValue = 'noImport'
    disabledValue.push('new', 'replace')
  } else if (!v.overwritable) {
    defaultValue = 'new'
    disabledValue.push('replace')
  } else {
    defaultValue = 'replace'
  }
  return { defaultValue, disabledValue }
}

function assemblyImportData (_data) {
  const assignData = {}
  const pageSize = 30 // 折叠项每页展示的数量
  // 后端接口返回字段归类
  const typeMaps = {
    tables: ['MODEL_TABLE', 'MODEL_FACT', 'MODEL_DIM'],
    columns: ['TABLE_COLUMN'],
    partitionColumns: ['MODEL_PARTITION', 'MODEL_MULTIPLE_PARTITION'],
    measures: ['MODEL_MEASURE'],
    dimensions: ['MODEL_DIMENSION'],
    indexes: ['WHITE_LIST_INDEX', 'TO_BE_DELETED_INDEX', 'RULE_BASED_INDEX'],
    computedColumns: ['MODEL_CC'],
    modelJoin: ['MODEL_JOIN'],
    modelFilter: ['MODEL_FILTER']
  }
  // 未找到、新增、删除以及更改对应需要展示的折叠项
  const dataMap = {
    nofound: ['tables', 'columns'],
    add: ['tables', 'modelJoin', 'columns', 'partitionColumns', 'measures', 'dimensions', 'indexes', 'computedColumns', 'modelFilter'],
    reduce: ['tables', 'modelJoin', 'columns', 'partitionColumns', 'measures', 'dimensions', 'indexes', 'computedColumns', 'modelFilter'],
    modified: ['columns', 'partitionColumns', 'measures', 'computedColumns', 'indexes', 'modelJoin', 'modelFilter']
  }

  // tab 接口字段对照
  const paneTabMap = {
    new_items: 'add',
    missing_items: 'nofound',
    reduce_items: 'reduce',
    update_items: 'modified'
  }

  for (let item in paneTabMap) {
    let v = paneTabMap[item]
    if (_data[item] && _data[item].length && item !== 'missing_items') {
      dataMap[v].forEach(name => {
        if (name === 'indexes') {
          const indexList = _data[item].filter(it => typeMaps[name].includes(it.type))
          assignData[v] = {
            ...assignData[v],
            [name]: {
              agg: indexList.filter(item => (item.detail || item.first_detail) < 20000000000).map(it => (it.detail || it.first_detail)).join(','),
              table: indexList.filter(item => (item.detail || item.first_detail) > 20000000000).map(it => (it.detail || it.first_detail)).join(','),
              length: indexList.length
            }
          }
        } else {
          let dataList = _data[item].filter(it => typeMaps[name].includes(it.type))
          assignData[v] = {
            ...assignData[v],
            [name]: {
              list: dataList.slice(0, pageSize),
              totalData: dataList,
              pageOffset: 1,
              pageSize: pageSize
            }
          }
        }
      })
    } else if (_data[item] && _data[item].length && item === 'missing_items') {
      dataMap[v].forEach(name => {
        let dataList = _data[item].filter(it => typeMaps[name].includes(it.type))
        assignData[v] = {...assignData[v],
          [name]: {
            list: dataList.slice(0, pageSize),
            totalData: dataList,
            pageOffset: 1,
            pageSize: pageSize
          }
        }
      })
    }
  }
  return assignData
}

export default {
  state: getInitialState(),
  mutations: {
    [actionTypes.SHOW_MODAL] (state) {
      state.isShow = true
    },
    [actionTypes.HIDE_MODAL] (state) {
      state.isShow = false
    },
    [actionTypes.SET_MODAL] (state, payload) {
      for (const [key, value] of Object.entries(payload)) {
        state[key] = value
      }
    },
    [actionTypes.INIT_MODAL] (state) {
      for (const [key, value] of Object.entries(getInitialState())) {
        state[key] = value
      }
    },
    [actionTypes.SET_MODAL_FORM] (state, payload) {
      for (const [key, value] of Object.entries(payload)) {
        state.form[key] = value
      }
    }
  },
  actions: {
    [actionTypes.CALL_MODAL] ({ commit }, payload) {
      return new Promise(resolve => {
        commit(actionTypes.INIT_MODAL)
        commit(actionTypes.SET_MODAL, { ...payload, callback: resolve })
        commit(actionTypes.SHOW_MODAL)
      })
    },
    [actionTypes.UPLOAD_MODEL_METADATA_FILE] ({ commit }, payload) {
      return new Promise(async (resolve, reject) => {
        const { project } = payload

        try {
          const form = formatUploadData(payload.form)
          const response = await api.model.uploadModelsMetadata({ project, form })
          const result = await handleSuccessAsync(response)

          const { models } = result
          const modelList = Object.keys(models).map(name => {
            const otherData = assemblyImportData(models[name])
            return {
              original_name: name,
              target_name: name,
              isNameError: false,
              nameErrorMsg: '',
              ...models[name],
              action: getDefaultAction(models[name]).defaultValue,
              ...otherData
            }
          })
          commit(actionTypes.SET_MODAL, { models: modelList })
          resolve()
        } catch (e) {
          // handleError(e)
          reject(e)
        }
      })
    },
    [actionTypes.IMPORT_MODEL_METADATA_FILE] ({ commit }, payload) {
      return new Promise(async (resolve, reject) => {
        const { project } = payload
        const form = formatImportData(payload.form)

        try {
          await api.model.importModelsMetadata({ project, form })
          resolve()
        } catch (e) {
          handleError(e)
          reject(e)
        }
      })
    }
  },
  getters: {
    modelNamesMap (state) {
      return Object
        .values(state.models)
        .reduce((map, model) => ({ ...map, [model.name]: model }), {})
    },
    conflictModels (state, getters) {
      const conflictModelsMap = {}
      const { modelNamesMap } = getters

      for (const conflict of state.conflicts) {
        // 只有前端允许的冲突类型，才可以成为冲突项
        if (conflict.type in conflictTypes) {
          // 如果conflict.items中有一项element隶属于zip解析后的某个model，则归类到map中
          const conflictModel = findConflictModelInZip(conflict.items, modelNamesMap)
          if (conflictModelsMap[conflictModel]) {
            conflictModelsMap[conflictModel].push(conflict)
          } else {
            conflictModelsMap[conflictModel] = [conflict]
          }
        }
      }
      // 把map整理为数组，随同id和conflicts一起导出
      return Object.entries(conflictModelsMap).map(model => {
        const [name, conflicts] = model
        const id = modelNamesMap[name].id
        return { id, name, conflicts }
      })
    },
    unConflictModels (state, getters) {
      const { conflictModels } = getters

      return state.models
        .filter(model => conflictModels.every(conflictModel => conflictModel.name !== model.name))
    },
    brokenConflictModels (state, getters) {
      const { conflictModels } = getters
      return conflictModels.filter(model => (
        model.conflicts.some(conflict => brokenConflictTypes.includes(conflict.type))
      ))
    },
    importableConflictModels (state, getters) {
      const { conflictModels } = getters
      return conflictModels.filter(model => (
        model.conflicts.every(conflict => importableConflictTypes.includes(conflict.type))
      ))
    }
  },
  namespaced: true
}
