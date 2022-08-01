import { set } from '../../../util/object'
import { sourceTypes } from '../../../config'
export const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL',
  INIT_FORM: 'INIT_FORM',
  UPDATE_JDBC_CONFIG: 'UPDATE_JDBC_CONFIG'
}

const initialState = JSON.stringify({
  isShow: false,
  callback: null,
  editType: '',
  firstEditType: '',
  form: {
    csvSettings: {
      url: '', // 路径
      type: '', // 存储位置类型
      accessKey: '',
      secretKey: '',
      addTableType: 0, // 添加csv的类型，0-向导模式 1-智能模式
      credentailType: 'KEY',
      separatorChar: ',', // 分隔符
      quoteChar: '', // 文本标识符
      name: '', // 表名
      ddl: '', // sql语句
      useFirstLine: false,
      sampleData: [],
      tableData: { // 表数据
        name: '',
        database: '',
        source_type: sourceTypes.CSV,
        columns: []
      }
    },
    project: null,
    selectedTables: [],
    selectedDatabases: [],
    needSampling: true,
    samplingRows: 20000000,
    settings: {
      type: '',
      name: '',
      creator: '',
      description: '',
      host: '',
      port: '',
      isAuthentication: false,
      username: '',
      password: ''
    },
    kafkaMeta: null,
    convertData: null,
    sampleData: null,
    treeData: [],
    columnData: null,
    connectGbaseSetting: {
      connectionString: '',
      username: '',
      password: '',
      synced: false
    }
  },
  databaseSizeObj: null,
  datasource: null,
  project: null
})

export default {
  state: JSON.parse(initialState),
  mutations: {
    [types.SET_MODAL_FORM]: (state, payload) => {
      state.form = { ...state.form, ...payload }
    },
    [types.SHOW_MODAL]: (state) => {
      state.isShow = true
    },
    [types.HIDE_MODAL]: (state) => {
      state.isShow = false
    },
    [types.SET_MODAL]: (state, payload) => {
      for (const key in payload) {
        state[key] = payload[key]
      }
    },
    [types.INIT_FORM]: (state) => {
      state.form = JSON.parse(initialState).form
      state.form.project = _getEmptySourceProject(state.project, state.editType)
      state.datasource && _getDatasourceSettings(state)
    },
    [types.UPDATE_JDBC_CONFIG] (state, sourceType) {
      const { override_kylin_properties } = state.project
      if (!override_kylin_properties) return
      if (+override_kylin_properties['kylin.source.default'] === sourceType &&
        override_kylin_properties['kylin.source.jdbc.dialect'] === 'gbase8a' &&
        override_kylin_properties['kylin.source.jdbc.connection-url'] &&
        override_kylin_properties['kylin.source.jdbc.pass'] &&
        override_kylin_properties['kylin.source.jdbc.user']
      ) {
        state.form.connectGbaseSetting = {
          ...state.form.connectGbaseSetting,
          connectionString: override_kylin_properties['kylin.source.jdbc.connection-url'],
          username: override_kylin_properties['kylin.source.jdbc.user'],
          password: override_kylin_properties['kylin.source.jdbc.pass'],
          synced: true
        }
      }
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { editType, project, datasource, databaseSizeObj }) {
      return new Promise(resolve => {
        commit(types.SET_MODAL, { editType, project, firstEditType: editType, datasource, databaseSizeObj, callback: resolve })
        commit(types.INIT_FORM)
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

function _getEmptySourceProject (project, editType) {
  const properties = { ...project.override_kylin_properties }
  project = set(project, 'override_kylin_properties', properties)
  project.override_kylin_properties['kylin.source.default'] = !isNaN(editType) ? editType : null
  return project
}

function _getDatasourceSettings (state) {
  state.form.settings.name = state.datasource.name
  state.form.settings.type = state.datasource.type
  state.form.settings.host = state.datasource.host
  state.form.settings.port = state.datasource.port
  state.form.project.override_kylin_properties['kylin.source.default'] = state.datasource.sourceType
}
