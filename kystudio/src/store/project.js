import api from './../service/api'
import * as types from './types'
import { cacheSessionStorage, cacheLocalStorage } from 'util/index'
import { getAvailableOptions } from '../util/specParser'
import { fromObjToArr } from '../util'
import { handleError } from '../util/business'

export default {
  state: {
    projectList: [],
    allProject: [],
    projectTotalSize: 0,
    projectAutoApplyConfig: false,
    isAllProject: false,
    selected_project: cacheSessionStorage('projectName') || cacheLocalStorage('projectName'),
    projectDefaultDB: '',
    isSemiAutomatic: false,
    projectPushdownConfig: true,
    snapshot_manual_management_enabled: false,
    scd2_enabled: false,
    second_storage_enabled: false,
    emptySegmentEnable: false,
    projectConfig: null,
    multi_partition_enabled: false,
    allConfig: [],
    defaultConfigList: [] // 不能操作的项目配置
  },
  mutations: {
    [types.SAVE_PROJECT_LIST]: function (state, { list, size }) {
      state.projectList = list
      state.projectTotalSize = size
    },
    [types.SAVE_CONFIG_LIST]: function (state, { list }) {
      state.allConfig = list
    },
    [types.SAVE_DEFAULT_CONFIG_LIST]: function (state, { list }) {
      state.defaultConfigList = list
    },
    [types.UPDATE_PROJECT_SEMI_AUTOMATIC_STATUS]: function (state, result) {
      state.isSemiAutomatic = result
    },
    [types.SET_PROJECT]: function (state, project) {
      cacheSessionStorage('preProjectName', state.selected_project) // 储存上一次选中的project
      cacheSessionStorage('projectName', project)
      cacheLocalStorage('projectName', project)
      state.selected_project = project
    },
    [types.CACHE_ALL_PROJECTS]: function (state, { list, size }) {
      state.allProject.splice(0, state.allProject.length)
      state.allProject = Object.assign(state.allProject, list)
      var selectedProject = cacheSessionStorage('projectName') || cacheLocalStorage('projectName')
      var hasMatch = false
      if (list && list.length > 0) {
        list.forEach((p) => {
          if (p.name === selectedProject) {
            hasMatch = true
            state.selected_project = p.name // 之前没这句，在其他tab 切换了project，顶部会不变
            state.projectDefaultDB = p.default_database
          }
        })
        if (!hasMatch) {
          state.selected_project = state.allProject[0].name
          cacheSessionStorage('projectName', state.selected_project)
          cacheLocalStorage('projectName', state.selected_project)
          state.projectDefaultDB = state.allProject[0].default_database
        }
      } else {
        cacheSessionStorage('projectName', '')
        cacheLocalStorage('projectName', '')
        state.selected_project = ''
        state.isSemiAutomatic = false // 如果接口没取到，默认设为false
        state.projectDefaultDB = ''
      }
    },
    [types.REMOVE_ALL_PROJECTS]: function (state) {
      state.allProject = []
    },
    [types.RESET_PROJECT_STATE]: function (state) {
      var selectedProject = cacheSessionStorage('projectName') || cacheLocalStorage('projectName')
      state.projectList.splice(0, state.projectList.length)
      state.allProject.splice(0, state.allProject.length)
      state.projectTotalSize = 0
      state.selected_project = selectedProject
      state.projectAccess = {}
      state.projectEndAccess = {}
    },
    [types.UPDATE_PROJECT] (state, { project }) {
      const projectIdx = state.allProject.findIndex(projectItem => projectItem.uuid === project.uuid)
      state.allProject = [
        ...state.allProject.slice(0, projectIdx),
        project,
        ...state.allProject.slice(projectIdx + 1)
      ]
    },
    [types.CACHE_PROJECT_TIPS_CONFIG]: function (state, { projectAutoApplyConfig }) {
      state.projectAutoApplyConfig = projectAutoApplyConfig
    },
    [types.CACHE_PROJECT_DEFAULT_DB]: function (state, { projectDefaultDB }) {
      state.projectDefaultDB = projectDefaultDB
    },
    [types.CACHE_PROJECT_PUSHDOWN_CONFIG]: function (state, { projectPushdownConfig }) {
      state.projectPushdownConfig = projectPushdownConfig
    },
    [types.UPDATE_SNAPSHOT_MANUAL_ENABLE] (state, type) {
      state.snapshot_manual_management_enabled = type
      if (this.state.config.platform === 'iframe') {
        window.parent.postMessage(`enableSnapshot:${type}`, '*')
      }
    },
    [types.UPDATE_SCD2_ENABLE] (state, type) {
      state.scd2_enabled = type
    },
    [types.UPDATE_MULTI_PARTITION_ENABLE] (state, type) {
      state.multi_partition_enabled = type
    },
    [types.UPDATE_EMPTY_SEGMENT_ENABLE]: function (state, emptySegmentEnable) {
      state.emptySegmentEnable = emptySegmentEnable
    },
    [types.CACHE_PROJECT_CONFIG]: function (state, projectConfig) {
      state.projectConfig = projectConfig
    }
  },
  actions: {
    [types.LOAD_PROJECT_LIST]: function ({ commit }, params) {
      return api.project.getProjectList(params).then((response) => {
        commit(types.SAVE_PROJECT_LIST, {list: response.data.data.value, size: response.data.data.total_size})
      })
    },
    [types.LOAD_CONFIG_BY_PROJEECT] ({ dispatch, commit }, params) {
      return api.project.getProjectList(params).then((response) => {
        let configObj = response.data.data.value.length ? response.data.data.value[0].override_kylin_properties : {}
        let list = fromObjToArr(configObj)
        commit(types.SAVE_CONFIG_LIST, {list})
        dispatch(types.GET_DEFAULT_CONFIG)
      })
    },
    [types.GET_DEFAULT_CONFIG] ({ commit }) {
      return api.project.getDefaultConfig().then(response => {
        let list = response.data.data
        commit(types.SAVE_DEFAULT_CONFIG_LIST, {list})
      }).catch((err) => {
        handleError(err)
      })
    },
    [types.UPDATE_PROJECT_CONFIG]: function ({ commit }, params) {
      return api.project.updateConfig(params)
    },
    [types.DELETE_PROJECT_CONFIG]: function ({ commit }, params) {
      return api.project.deleteConfig(params)
    },
    [types.LOAD_ALL_PROJECT]: function ({ dispatch, commit, state }, params) {
      return new Promise((resolve, reject) => {
        api.project.getProjectList({page_offset: 0, page_size: 100000}).then((response) => {
          commit(types.CACHE_ALL_PROJECTS, {list: response.data.data.value})
          let pl = response.data.data && response.data.data.value && response.data.data.value.length || 0
          if (!((params && params.ignoreAccess) || pl === 0)) {
            let curProjectUserAccessPromise = dispatch(types.USER_ACCESS, {project: state.selected_project})
            let curProjectConfigPromise = dispatch(types.FETCH_PROJECT_SETTINGS, {projectName: state.selected_project})
            Promise.all([curProjectUserAccessPromise, curProjectConfigPromise]).then(() => {
              resolve(response.data.data.value)
            }, () => {
              resolve(response.data.data.value)
            })
          } else {
            resolve(response.data.data.value)
          }
        }, () => {
          commit(types.REMOVE_ALL_PROJECTS)
          reject()
        })
      })
    },
    [types.DELETE_PROJECT]: function ({ commit }, projectName) {
      return api.project.deleteProject(projectName)
    },
    [types.UPDATE_PROJECT]: function ({ commit }, project) {
      return api.project.updateProject(project)
        .then(response => {
          commit(types.UPDATE_PROJECT, { project: response.data.data })
        })
    },
    [types.SAVE_PROJECT]: function ({ dispatch, commit }, project) {
      return api.project.saveProject(project).then(async (res) => {
        // await dispatch(types.LOAD_ALL_PROJECT)
        return res
      })
    },
    [types.SAVE_PROJECT_ACCESS]: function ({ commit }, {accessData, id}) {
      return api.project.addProjectAccess(accessData, id)
    },
    [types.EDIT_PROJECT_ACCESS]: function ({ commit }, {accessData, id}) {
      return api.project.editProjectAccess(accessData, id)
    },
    [types.GET_PROJECT_ACCESS]: function ({ commit }, para) {
      return api.project.getProjectAccess(para.project_id, para.data)
    },
    [types.DEL_PROJECT_ACCESS]: function ({ commit }, {id, aid, userName, principal}) {
      return api.project.delProjectAccess(id, aid, userName, principal)
    },
    [types.SUBMIT_ACCESS_DATA]: function ({ commit }, {projectName, userType, roleOrName, accessData}) {
      return api.project.submitAccessData(projectName, userType, roleOrName, accessData)
    },
    [types.ADD_PROJECT_FILTER]: function ({ commit }, filterData) {
      return api.project.saveProjectFilter(filterData)
    },
    [types.EDIT_PROJECT_FILTER]: function ({ commit }, filterData) {
      return api.project.updateProjectFilter(filterData)
    },
    [types.DEL_PROJECT_FILTER]: function ({ commit }, {project, filterName}) {
      return api.project.delProjectFilter(project, filterName)
    },
    [types.GET_PROJECT_FILTER]: function ({ commit }, project) {
      return api.project.getProjectFilter(project)
    },
    [types.BACKUP_PROJECT]: function ({ commit }, project) {
      return api.project.backupProject(project)
    },
    [types.ACCESS_AVAILABLE_USER_OR_GROUP]: function ({ commit }, para) {
      return api.project.accessAvailableUserOrGroup(para.type, para.uuid, para.data)
    },
    [types.GET_QUOTA_INFO]: function ({ commit }, para) {
      return api.project.getQuotaInfo(para)
    },
    [types.CLEAR_TRASH]: function ({ commit }, para) {
      return api.project.clearTrash(para)
    },
    [types.FETCH_PROJECT_SETTINGS]: function ({ commit }, para) {
      return api.project.fetchProjectSettings(para.projectName).then((response) => {
        // commit(types.CACHE_PROJECT_TIPS_CONFIG, {projectAutoApplyConfig: response.data.data.tips_enabled})
        commit(types.CACHE_PROJECT_CONFIG, {projectDefaultDB: response.data.data})
        commit(types.CACHE_PROJECT_DEFAULT_DB, {projectDefaultDB: response.data.data.default_database})
        commit(types.CACHE_PROJECT_PUSHDOWN_CONFIG, {projectPushdownConfig: response.data.data.push_down_enabled})
        commit(types.UPDATE_SCD2_ENABLE, response.data.data.scd2_enabled || false)
        commit(types.UPDATE_SNAPSHOT_MANUAL_ENABLE, response.data.data.snapshot_manual_management_enabled || false)
        commit(types.UPDATE_MULTI_PARTITION_ENABLE, response.data.data.multi_partition_enabled || false)
        commit(types.UPDATE_EMPTY_SEGMENT_ENABLE, response.data.data.create_empty_segment_enabled || false)
        return response
      })
    },
    [types.FETCH_AVAILABLE_NODES]: function ({ commit }, para) {
      return api.project.fetchAvailableNodes(para)
    },
    [types.UPDATE_PROJECT_GENERAL_INFO]: function ({ commit }, para) {
      return api.project.updateProjectGeneralInfo(para)
    },
    [types.UPDATE_SEGMENT_CONFIG]: function ({ commit }, para) {
      return api.project.updateSegmentConfig(para)
    },
    [types.UPDATE_PUSHDOWN_CONFIG]: function ({commit}, para) {
      return api.project.updatePushdownConfig(para).then((response) => {
        commit(types.CACHE_PROJECT_PUSHDOWN_CONFIG, {projectPushdownConfig: para.push_down_enabled})
        return response
      })
    },
    [types.UPDATE_STORAGE_QUOTA]: function ({commit}, para) {
      return api.project.updateStorageQuota(para)
    },
    [types.UPDATE_JOB_ALERT_SETTINGS]: function ({commit}, para) {
      return api.project.updateJobAlertSettings(para)
    },
    [types.UPDATE_PROJECT_DATASOURCE]: function ({commit}, para) {
      return api.project.updateProjectDatasource(para)
    },
    [types.RESET_PROJECT_CONFIG]: function ({ commit }, para) {
      return api.project.resetConfig(para)
    },
    [types.UPDATE_DEFAULT_DB_SETTINGS]: function ({ commit }, para) {
      return api.project.updateDefaultDBSettings(para)
    },
    [types.UPDATE_YARN_QUEUE]: function (_, para) {
      return api.project.updateYarnQueue(para)
    },
    [types.UPDATE_SNAPSHOT_CONFIG]: function ({ commit }, para) {
      return api.project.updateSnapshotConfig(para)
    },
    [types.UPDATE_EXPOSE_CC_CONFIG]: function ({ commit }, para) {
      return api.project.updateExposeCCConfig(para)
    },
    [types.GET_ACL_PERMISSION]: function (_, para) {
      return api.project.getAclPermission(para)
    },
    [types.UPDATE_KERBEROS_CONFIG]: function ({ commit }, para) {
      return api.project.updateKerberosConfig(para)
    },
    [types.GET_AVAILABLE_PROJECT_OWNERS]: function ({ commit }, para) {
      return api.project.getAvailableProjectOwners(para)
    },
    [types.UPDATE_PROJECT_OWNER]: function ({ commit }, para) {
      return api.project.updateProjectOwner(para)
    },
    [types.UPDATE_INDEX_OPTIMIZATION]: function ({ commit }, para) {
      return api.project.updateIndexOptimization(para)
    },
    [types.UPDATE_FAVORITE_RULES]: function (_, para) {
      return api.project.updateFavoriteRules(para)
    },
    [types.TOGGLE_ENABLE_SCD]: function ({ commit }, para) {
      return api.project.toggleEnableSCD(para)
    },
    [types.GET_SCD2_MODEL]: function ({ commit }, para) {
      return api.project.getSCDModel(para)
    },
    [types.LOAD_PROJECT_STATISTICS]: function ({ commit }, para) {
      return api.project.loadStatistics(para)
    },
    [types.TOGGLE_MULTI_PARTITION]: function ({ commit }, para) {
      return api.project.toggleMultiPartition(para)
    },
    [types.GET_MULTI_PARTITION_MODEL]: function ({ commit }, para) {
      return api.project.getMultiPartitionModels(para)
    }
  },
  getters: {
    projectActions: (state, getters, rootState, rootGetters) => {
      const groupRole = rootGetters.userAuthorities
      const projectRole = rootState.user.currentUserAccess

      return getAvailableOptions('projectActions', { groupRole, projectRole })
    },
    settingActions: (state, getters, rootState, rootGetters) => {
      const groupRole = rootGetters.userAuthorities
      const projectRole = rootState.user.currentUserAccess

      return getAvailableOptions('settingActions', { groupRole, projectRole })
    },
    currentSelectedProject: (state) => {
      return state.selected_project
    },
    currentProjectData: (state, getters, rootState) => {
      const _filterable = state.allProject.filter(p => {
        return p.name === state.selected_project
      })
      if (Array.isArray(_filterable) && _filterable.length > 0) {
        return _filterable[0]
      }
      return {override_kylin_properties: {}}
    },
    selectedProjectDatasource: (state, getters, rootState) => {
      let datasourceKey = null

      const _filterable = state.allProject.filter(p => {
        return p.name === state.selected_project
      })
      if (Array.isArray(_filterable) && _filterable.length > 0) {
        datasourceKey = _filterable[0].override_kylin_properties['kylin.source.default']
      }

      return datasourceKey
    },
    globalDefaultDatasource: (state, getters, rootState) => {
      return rootState.system.sourceDefault
    },
    configList: (state, getters, rootState) => {
      const { allConfig, defaultConfigList } = state
      return allConfig.filter(v => defaultConfigList.findIndex(item => item === v.key) === -1)
    }
  }
}
