import api from './../service/api'
import * as types from './types'
import { getProperty } from '../util/business'
import { handleError } from 'util/business'
import Vue from 'vue'
export default {
  state: {
    messageDirectives: [],
    needReset: false,
    authentication: null,
    adminConfig: null,
    serverConfig: null,
    serverEnvironment: null,
    serviceState: [],
    serverAboutKap: {},
    canaryReport: {},
    timeZone: '',
    securityProfile: 'testing',
    removeSecurityLimit: 'false',
    limitlookup: 'true',
    strategy: 'default',
    showHtrace: false,
    hiddenRaw: true,
    hiddenExtendedColumn: true,
    storage: 2,
    engine: 2,
    allowAdminExport: 'true',
    allowNotAdminExport: 'true',
    enableStackTrace: 'false',
    filterUserName: '', // group页面 选择用户组件使用
    canaryReloadTimer: 15,
    sourceDefault: 0,
    lang: 'en',
    platform: '',
    guideConfig: {
      guideType: '',
      guideModeCheckDialog: false,
      globalMouseDrag: false,
      globalMaskVisible: false,
      globalMouseVisible: false,
      globalMouseClick: false,
      mousePos: {
        x: 0,
        y: 0
      },
      targetList: {}
    },
    showLisenceSuccessDialog: false,
    loadHiveTableNameEnabled: 'true',
    kerberosEnabled: 'false',
    jobLogs: '',
    showRevertPasswordDialog: 'true',
    isEditForm: false,
    isShowGlobalAlter: false,
    dimMeasNameMaxLength: 300,
    favoriteImportSqlMaxSize: 1000,
    resourceGroupEnabled: 'false',
    queryDownloadMaxSize: 100000,
    isShowSecondStorage: false,
    isNonAdminGenQueryDiagPackage: 'true',
    streamingEnabled: 'false',
    storageQuery: 'true'
  },
  mutations: {
    [types.COLLECT_MESSAGE_DIRECTIVES]: (state, directive) => {
      state.messageDirectives.push(directive)
    },
    [types.SAVE_AUTHENTICATION]: function (state, result) {
      state.authentication = result.authentication
    },
    [types.SAVE_ENV]: function (state, result) {
      state.serverEnvironment = result.env
    },
    [types.SAVE_CONF]: function (state, result) {
      state.serverConfig = result.conf
    },
    [types.SAVE_ADMIN_CONF]: function (state, result) {
      state.adminConfig = result.conf
    },
    [types.GET_CONF_BY_NAME]: function (state, {name, key, defaultValue}) {
      const v = getProperty(name, state.serverConfig)
      // 配置为空时(没有匹配到），转为默认配置
      if (!v && typeof defaultValue !== 'undefined') {
        state[key] = defaultValue
      } else {
        state[key] = v
      }
      return state[key]
    },
    [types.SAVE_INSTANCE_CONF_BY_NAME]: function (state, {key, val}) {
      state[key] = val
      if (key === 'timeZone') {
        localStorage.setItem('GlobalSeverTimeZone', state[key])
      }
    },
    [types.GET_ABOUT]: function (state, result) {
      state.serverAboutKap = result.list.data
      let version = /^[^/d]+?(\d).*$/.exec(result.list.data['ke.version'])
      state.serverAboutKap['version'] = version && version.length >= 2 ? version[1] : '4'
      state.serverAboutKap['msg'] = result.list.msg
      state.serverAboutKap['code'] = result.list.code
    },
    [types.SAVE_SERVICE_STATE]: function (state, result) {
      state.serviceState = result.list
    },
    [types.SAVE_CANARY_REPORT]: function (state, result) {
      state.canaryReport = result.list
    },
    [types.SET_CHANGED_FORM]: function (state, result) {
      state.isEditForm = result
    },
    [types.SET_GLOBAL_ALTER]: function (state, result) {
      state.isShowGlobalAlter = result
    }
  },
  actions: {
    [types.LOAD_AUTHENTICATION]: function ({ commit }) {
      /* api.system.getAuthentication().then((response) => {
        commit(types.SAVE_AUTHENTICATION, { authentication: response.data })
      }) */
      return new Promise((resolve, reject) => {
        api.system.getAuthentication().then((response) => {
          if ((!response || !response.data || response.data.data === null)) {
            window.kylinVm.$router.replace('/access/login')
            return
          }
          commit(types.SAVE_AUTHENTICATION, { authentication: response.data })
          commit(types.INIT_SPEC)
          resolve(response.data)
        }, (res) => {
          reject(res)
          if (window.kylinVm.$route.path === '/') {
            window.localStorage.setItem('loginIn', false)
            window.kylinVm.$router.replace('/access/login')
          }
          if (res.status !== 401) {
            Vue.config.lang = localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : 'en'
            handleError(res)
          }
        })
      })
    },
    [types.GET_ADMIN_CONFIG]: function ({ commit }) {
      return api.system.getConfig().then((response) => {
        commit(types.SAVE_ADMIN_CONF, { conf: response.data.data })
        return response
      })
    },
    [types.GET_ENV]: function ({ commit }) {
      return api.system.getEnv().then((response) => {
        commit(types.SAVE_ENV, { env: response.data.data })
        return response
      })
    },
    [types.GET_CONF]: function ({ commit }) {
      return new Promise((resolve, reject) => {
        api.system.getPublicConfig().then((response) => {
          commit(types.SAVE_CONF, { conf: response.data.data })
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.security.profile', key: 'securityProfile'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.security.remove-ldap-custom-security-limit-enabled', key: 'removeSecurityLimit'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.htrace.show-gui-trace-toggle', key: 'showHtrace'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.web.hide-feature.raw-measure', key: 'hiddenRaw'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.web.hide-feature.extendedcolumn-measure', key: 'hiddenExtendedColumn'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.engine.default', key: 'engine'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.storage.default', key: 'storage'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.web.export-allow-admin', key: 'allowAdminExport'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.web.export-allow-other', key: 'allowNotAdminExport'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.web.stack-trace.enabled', key: 'enableStackTrace'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.canary.default-canaries-period-min', key: 'canaryReloadTimer'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.source.default', key: 'sourceDefault'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.source.load-hive-tablename-enabled', key: 'loadHiveTableNameEnabled', defaultValue: 'true'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.kerberos.project-level-enabled', key: 'kerberosEnabled'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.metadata.random-admin-password.enabled', key: 'showRevertPasswordDialog', defaultValue: 'true'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.model.dimension-measure-name.max-length', key: 'dimMeasNameMaxLength', defaultValue: 300})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.model.suggest-model-sql-limit', key: 'favoriteImportSqlMaxSize'})
          commit(types.GET_CONF_BY_NAME, {name: 'resource_group_enabled', key: 'resourceGroupEnabled', defaultValue: 'false'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.query.query-history-download-max-size', key: 'queryDownloadMaxSize', defaultValue: 100000})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.second-storage.class', key: 'isShowSecondStorage'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.security.allow-non-admin-generate-query-diag-package', key: 'isNonAdminGenQueryDiagPackage', defaultValue: 'true'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.web.sso-redirect-url', key: 'ssoRedirectUrl'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.sso.cookie-name', key: 'cookieNames'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.sso.sso-login-enabled', key: 'ssoLoginEnabled'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.model.measure-name-check-enabled', key: 'enableCheckName', defaultValue: 'true'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.streaming.enabled', key: 'streamingEnabled', defaultValue: 'false'})
          commit(types.GET_CONF_BY_NAME, {name: 'kylin.second-storage.query-metric-collect', key: 'storageQuery', defaultValue: 'true'})
          resolve(response)
        }, () => {
          reject()
        })
      })
    },
    [types.GET_INSTANCE_CONF]: function ({ commit }) {
      return new Promise((resolve, reject) => {
        api.system.getInstanceConfig().then((response) => {
          try {
            commit(types.SAVE_INSTANCE_CONF_BY_NAME, {key: 'timeZone', val: response.data.data['instance.timezone']})
            resolve(response)
          } catch (e) {
            reject(e)
          }
        }, () => {
          reject()
        })
      })
    },
    [types.RELOAD_METADATA]: function ({ commit }) {
      return api.system.reloadMetadata()
    },
    [types.BACKUP_METADATA]: function ({ commit }) {
      return api.system.backupMetadata()
    },
    [types.UPDATE_CONFIG]: function ({ commit }, config) {
      return api.system.updateConfig(config)
    },
    [types.GET_CANARY_REPORT]: function ({ commit }, para) {
      return api.system.getCanaryReport(para).then((response) => {
        commit(types.SAVE_CANARY_REPORT, { list: response.data.data })
      })
    },
    [types.DOWNLOAD_LOGS]: function ({commit}, para) {
      return api.system.downloadJobLogs(para)
    }
  },
  getters: {
    isTestingSecurityProfile (state) {
      return state.securityProfile === 'testing' || state.removeSecurityLimit === 'true'
    },
    isGuideMode (state) {
      return state.guideConfig.globalMaskVisible
    },
    dimMeasNameMaxLength: (state) => {
      return state.dimMeasNameMaxLength
    },
    queryDownloadMaxSize: (state) => {
      return state.queryDownloadMaxSize
    },
    isNonAdminGenQueryDiagPackage: (state) => {
      return state.isNonAdminGenQueryDiagPackage === 'true'
    },
    isStreamingEnabled: (state) => {
      return state.streamingEnabled === 'true'
    }
  }
}

