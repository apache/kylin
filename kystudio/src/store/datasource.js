import api from './../service/api'
import { indexOfObjWithSomeKey } from 'util'
import * as types from './types'
import { getAvailableOptions } from '../util/specParser'
export default {
  state: {
    dataSource: {},
    encodings: null,
    encodingMatchs: null,
    encodingCache: {},
    currentShowTableData: null,
    editableTabs: null,
    mapHasTables: {}
  },
  getters: {
    getQueryTabs (state) {
      return state.editableTabs
    },
    datasourceActions (state, getters, rootState, rootGetters) {
      const groupRole = rootGetters.userAuthorities
      const projectRole = rootState.user.currentUserAccess
      return getAvailableOptions('datasourceActions', { groupRole, projectRole })
    },
    metadataActions (state, getters, rootState, rootGetters) {
      const groupRole = rootGetters.userAuthorities
      const projectRole = rootState.user.currentUserAccess
      return getAvailableOptions('metadataActions', { groupRole, projectRole })
    },
    modelActions (state, getters, rootState, rootGetters) {
      const groupRole = rootGetters.userAuthorities
      const projectRole = rootState.user.currentUserAccess

      return getAvailableOptions('modelActions', { groupRole, projectRole })
    }
  },
  mutations: {
    [types.CLEAR_DATASOURCE_CACHE]: function (state, project) {
      state.mapHasTables = {}
      state.dataSource[project] = []
    },
    // 缓存数据源信息避免反复存储
    [types.CACHE_DATASOURCE]: function (state, { data, project, isReset = true }) {
      if (project) {
        state.dataSource[project] = state.dataSource[project] || []
        data.tables.forEach((t) => {
          let tableUid = project + t.database + t.name
          if (!state.mapHasTables[tableUid]) {
            state.dataSource[project].push(t)
            state.mapHasTables[tableUid] = true
          }
        })
      }
    },
    // 更新缓存
    [types.REPLACE_TABLE_CACHE]: function (state, {data, project}) {
      let databaseInfo = state.dataSource[project] || []
      let i = indexOfObjWithSomeKey(databaseInfo, 'name', data.name)
      databaseInfo.splice(i, 1)
      let tableUid = project + data.database + data.name
      state.mapHasTables[tableUid] = false
    },
    [types.CACHE_ENCODINGS]: function (state, { data, project }) {
      state.encodings = data
    },
    [types.CACHE_ENCODINGMATCHS]: function (state, { data, project }) {
      state.encodingMatchs = data
    },
    [types.SET_CURRENT_TABLE] (state, { tableData }) {
      state.currentShowTableData = tableData
    },
    [types.SET_QUERY_TABS] (state, { tabs }) {
      state.editableTabs = tabs
    },
    [types.RESET_QUERY_TABS]: (state, {projectName}) => {
      state.editableTabs && projectName in state.editableTabs && delete state.editableTabs[projectName]
    }
  },
  actions: {
    [types.FETCH_SNAPSHOT_LIST]: function ({ commit }, para) {
      return api.datasource.fetchSnapshotList(para)
    },
    [types.GET_SNAPSHOT_PARTITION_VALUES] (_, para) {
      return api.datasource.getSnapshotPartitionValues(para)
    },
    [types.FETCH_UNBUILD_SNAPSHOT_TABLES]: function ({ commit }, para) {
      return api.datasource.fetchUnbuildSnapshotTables(para)
    },
    [types.FETCH_DATABASE_MORE_TABLES]: function ({ commit }, para) {
      return api.datasource.fetchDatabaseMoreTables(para)
    },
    [types.BUILD_SNAPSHOT_TABLES]: function ({ commit }, para) {
      return api.datasource.buildSnapshotTables(para)
    },
    [types.REFRESH_SNAPSHOT_TABLE]: function ({ commit }, para) {
      return api.datasource.refreshSnapshotTable(para)
    },
    [types.DELETE_SNAPSHOT_CHECK]: function ({ commit }, para) {
      return api.datasource.deleteSnapshotCheck(para)
    },
    [types.DELETE_SNAPSHOT]: function ({ commit }, para) {
      return api.datasource.deleteSnapshot(para)
    },
    [types.FETCH_PARTITION_CONFIG]: function (_, para) {
      return api.datasource.fetchPartitionConfig(para)
    },
    [types.RELOAD_PARTITION_COLUMN]: function (_, para) {
      return api.datasource.reloadPartitionColumn(para)
    },
    [types.SAVE_PARTITION_COLUMN]: function (_, para) {
      return api.datasource.savePartitionColumn(para)
    },
    [types.LOAD_DATASOURCE_OF_MODEL]: function ({ comit }, para) {
      return api.datasource.loadDataSourceOfModel(para)
    },
    [types.LOAD_DATASOURCE]: function ({ commit }, para) {
      return api.datasource.loadDataSource(para.isExt, para.project, para.datasource)
        .then((response) => {
          commit(types.CACHE_DATASOURCE, { data: response.data.data, project: para.project })
          return response
        })
    },
    [types.RELOAD_DATASOURCE]: function ({commit}, para) {
      return api.datasource.reloadDataSource(para).then((response) => {
        commit(types.REPLACE_TABLE_CACHE, {
          data: {
            database: para.table.split('.')[0],
            name: para.table.split('.')[1]
          },
          project: para.project
        })
        return response
      })
    },
    [types.CHECK_BASE_CONFIG] (_, para) {
      return api.datasource.checkGbaseConfig(para)
    },
    [types.GET_RELOAD_INFLUENCE]: function ({commit}, para) {
      return api.datasource.getReloadInfluence(para)
    },
    [types.LOAD_DATASOURCE_EXT]: function ({commit}, para) {
      return api.datasource.loadDataSourceExt({ext: true, project: para.project, table: para.tableName})
    },
    [types.LOAD_HIVEBASIC_DATABASE_TABLES]: function ({commit}, para) {
      return api.datasource.loadBasicLiveDatabaseTables(para.projectName, para.sourceType, para.databaseName, para.table, para.page_offset, para.page_size)
    },
    [types.LOAD_HIVEBASIC_DATABASE]: function ({commit}, para) {
      return api.datasource.loadBasicLiveDatabase(para.projectName, para.sourceType)
    },
    [types.LOAD_HIVE_TABLES]: function ({commit}, para) {
      return api.datasource.loadChildTablesOfDatabase(para.projectName, para.sourceType, para.databaseName, para.tableName, para.page_offset, para.pageSize)
    },
    [types.LOAD_HIVE_IN_PROJECT]: function ({commit}, para) {
      return api.datasource.loadHiveInProject(para)
    },
    [types.SUBMIT_SAMPLING]: function ({commit}, para) {
      return api.datasource.submitSampling(para)
    },
    [types.HAS_SAMPLING_JOB]: function ({commit}, para) {
      return api.datasource.hasSamplingJob(para)
    },
    [types.UN_LOAD_HIVE_IN_PROJECT]: function ({commit}, data) {
      return api.datasource.unLoadHiveInProject(data)
    },
    [types.LOAD_BUILD_COMPLETE_TABLES]: function ({commit}, project) {
      return api.datasource.loadBuildCompeleteTables(project)
    },
    [types.QUERY_BUILD_TABLES]: function ({commit}, para) {
      return api.datasource.query(para)
    },
    // 停止查询
    [types.STOP_QUERY_BUILD]: (_, para) => {
      return api.datasource.stop(para)
    },
    [types.SAVE_QUERY]: function ({commit}, para) {
      return api.datasource.saveQuery(para)
    },
    [types.LOAD_ONLINE_QUERY_NODES]: function ({commit}, para) {
      return api.datasource.loadOnlineQueryNodes(para)
    },
    [types.GET_SAVE_QUERIES]: function ({commit}, para) {
      return api.datasource.getSaveQueries(para)
    },
    [types.DELETE_QUERY]: function ({commit}, para) {
      return api.datasource.deleteQuery(para)
    },
    [types.GET_RULES]: function ({commit}, para) {
      return api.datasource.getRules(para)
    },
    [types.GET_USER_AND_GROUPS]: function ({commit, rootState}, para) {
      const { project: { selected_project, allProject } } = rootState
      const projectId = allProject.filter(item => item.name === selected_project)[0].uuid
      return api.datasource.getUserAndGroupsByProjectAdmin(para, projectId)
    },
    [types.GET_RULES_IMPACT]: function ({commit}, para) {
      return api.datasource.getRulesImpact(para)
    },
    [types.GET_PREFERRENCE]: function ({commit}, para) {
      return api.datasource.getPreferrence(para)
    },
    [types.UPDATE_RULES]: function ({commit}, para) {
      return api.datasource.updateRules(para)
    },
    [types.UPDATE_PREFERRENCE]: function ({commit}, para) {
      return api.datasource.updatePreferrence(para)
    },
    [types.GET_HISTORY_LIST]: function ({commit}, para) {
      return api.datasource.getHistoryList(para)
    },
    [types.GET_FAVORITE_LIST]: function ({commit}, para) {
      return api.datasource.getFavoriteList(para)
    },
    [types.GET_WAITING_ACCE_SIZE]: function ({commit}, para) {
      return api.datasource.getWaitingAcceSize(para)
    },
    [types.IMPORT_SQL_FILES]: function ({commit}, para) {
      return api.datasource.importSqlFiles(para)
    },
    [types.FORMAT_SQL]: function ({ commit }, para) {
      return api.datasource.formatSql(para)
    },
    [types.VALIDATE_WHITE_SQL]: function ({commit}, para) {
      return api.datasource.validateWhite(para)
    },
    [types.ADD_TO_FAVORITE_LIST]: function ({commit}, para) {
      return api.datasource.addToFavoriteList(para)
    },
    [types.REMOVE_FAVORITE_SQL]: function ({commit}, para) {
      return api.datasource.removeFavSql(para)
    },
    [types.LOAD_BLACK_LIST]: function ({commit}, para) {
      return api.datasource.loadBlackList(para)
    },
    [types.DELETE_BLACK_SQL]: function ({commit}, para) {
      return api.datasource.deleteBlack(para)
    },
    [types.GET_CANDIDATE_LIST]: function ({commit}, para) {
      return api.datasource.getCandidateList(para)
    },
    [types.GET_ENCODINGS]: function ({commit}) {
      return api.datasource.getEncoding().then((response) => {
        commit(types.CACHE_ENCODINGS, {data: response.data.data})
        api.datasource.getEncodingMatchs().then((response) => {
          commit(types.CACHE_ENCODINGMATCHS, {data: response.data.data})
        })
      })
    },
    [types.COLLECT_SAMPLE_DATA]: function ({commit}, para) {
      return api.datasource.collectSampleData(para)
    },
    [types.GET_TABLE_JOB]: function ({commit}, para) {
      return api.datasource.getTableJob(para.tableName, para.project)
    },
    // acl table
    [types.GET_ACL_SET_TABLE]: function ({commit}, para) {
      return api.datasource.getAclOfTable(para.tableName, para.project, para.type, para.pager)
    },
    [types.SAVE_ACL_SET_TABLE]: function ({commit}, para) {
      return api.datasource.saveAclSetOfTable(para.tableName, para.project, para.userName, para.type)
    },
    [types.DEL_ACL_SET_TABLE]: function ({commit}, para) {
      return api.datasource.cancelAclSetOfTable(para.tableName, para.project, para.userName, para.type)
    },
    [types.GET_ACL_BLACKLIST_TABLE]: function ({commit}, para) {
      return api.datasource.getAclBlackListOfTable(para.tableName, para.project, para.type, para.other_para)
    },
    // acl column
    [types.GET_ACL_SET_COLUMN]: function ({commit}, para) {
      return api.datasource.getAclOfColumn(para.tableName, para.project, para.type, para.pager)
    },
    [types.SAVE_ACL_SET_COLUMN]: function ({commit}, para) {
      return api.datasource.saveAclSetOfColumn(para.tableName, para.project, para.userName, para.columns, para.type)
    },
    [types.UPDATE_ACL_SET_COLUMN]: function ({commit}, para) {
      return api.datasource.updateAclSetOfColumn(para.tableName, para.project, para.userName, para.columns, para.type)
    },
    [types.DEL_ACL_SET_COLUMN]: function ({commit}, para) {
      return api.datasource.cancelAclSetOfColumn(para.tableName, para.project, para.userName, para.type)
    },
    [types.GET_ACL_WHITELIST_COLUMN]: function ({commit}, para) {
      return api.datasource.getAclWhiteListOfColumn(para.tableName, para.project, para.type, para.other_para)
    },
    // acl row
    [types.GET_ACL_SET_ROW]: function ({commit}, para) {
      return api.datasource.getAclOfRow(para.tableName, para.project, para.type, para.pager)
    },
    [types.SAVE_ACL_SET_ROW]: function ({commit}, para) {
      return api.datasource.saveAclSetOfRow(para.tableName, para.project, para.userName, para.conditions, para.type)
    },
    [types.UPDATE_ACL_SET_ROW]: function ({commit}, para) {
      return api.datasource.updateAclSetOfRow(para.tableName, para.project, para.userName, para.conditions, para.type)
    },
    [types.DEL_ACL_SET_ROW]: function ({commit}, para) {
      return api.datasource.cancelAclSetOfRow(para.tableName, para.project, para.userName, para.type)
    },
    [types.GET_ACL_WHITELIST_ROW]: function ({commit}, para) {
      return api.datasource.getAclWhiteListOfRow(para.tableName, para.project, para.type, para.other_para)
    },
    [types.PREVIEW_ACL_SET_ROW_SQL]: function ({commit}, para) {
      return api.datasource.previewAclSetOfRowSql(para.tableName, para.project, para.userName, para.conditions)
    },
    [types.SAVE_TABLE_PARTITION]: function ({commit}, body) {
      return api.datasource.saveTablePartition(body)
    },
    [types.SAVE_DATA_RANGE]: function ({commit}, body) {
      return api.datasource.saveDataRange(body)
    },
    [types.FETCH_RELATED_MODELS]: function ({commit}, para) {
      return api.datasource.fetchRelatedModels(para.projectName, para.tableFullName, para.modelName, para.page_offset, para.pageSize)
    },
    [types.FETCH_DB_AND_TABLES]: function ({commit}, para) { // 获取db 和 tables，参数：projectName，页码，每页条数，搜索关键字，数据源类型
      return api.datasource.fetchDBandTables(para)
    },
    [types.FETCH_DATABASES]: function ({commit}, para) {
      return api.datasource.fetchDatabases(para.projectName, para.sourceType)
    },
    [types.FETCH_TABLES]: function ({commit}, para) {
      return api.datasource.fetchTables(para.projectName, para.databaseName, para.tableName, para.page_offset, para.pageSize, para.isFuzzy, para.sourceType, para.isExt)
        .then((response) => {
          if (para.isFuzzy && !para.isDisableCache) {
            commit(types.CACHE_DATASOURCE, { data: response.data.data, project: para.projectName, isReset: para.page_offset === 0 })
          }
          return response
        })
    },
    [types.RELOAD_HIVE_DB_TABLES]: function ({commit}, para) {
      return api.datasource.reloadHiveDBAndTables(para)
    },
    [types.UPDATE_TOP_TABLE]: function ({commit}, para) {
      return api.datasource.updateTopTable(para.projectName, para.tableFullName, para.isTopSet)
    },
    [types.DELETE_TABLE]: function ({commit}, para) {
      return api.datasource.deleteTable(para.projectName, para.databaseName, para.tableName, para.cascade)
    },
    [types.FETCH_CHANGE_TYPE_INFO]: function ({commit}, para) {
      return api.datasource.fetchChangeTypeInfo(para.projectName, para.tableName, para.affectedType)
    },
    [types.PREPARE_UNLOAD]: function ({ commit }, para) {
      return api.datasource.prepareUnload(para)
    },
    [types.FETCH_RANGE_FRESH_INFO]: function ({commit}, para) {
      return api.datasource.fetchRangeFreshInfo(para.projectName, para.tableFullName, String(para.startTime), String(para.endTime))
    },
    [types.FRESH_RANGE_DATA]: function ({commit}, para) {
      return api.datasource.freshRangeData(para.projectName, para.tableFullName, String(para.startTime), String(para.endTime), para.affected_start, para.affected_end)
    },
    [types.FETCH_MERGE_CONFIG]: function ({commit}, para) {
      return api.datasource.fetchMergeConfig(para.projectName, para.modelName, para.tableFullName)
    },
    [types.UPDATE_MERGE_CONFIG]: function ({commit}, para) {
      return api.datasource.updateMergeConfig(para.projectName, para.modelName, para.tableFullName, para.isAutoMerge, para.autoMergeConfigs, para.isVolatile, para.volatileConfig.value, para.volatileConfig.type)
    },
    [types.FETCH_PUSHDOWN_CONFIG]: function ({commit}, para) {
      return api.datasource.fetchPushdownConfig(para.projectName, para.tableFullName)
    },
    [types.DISCARD_TABLE_MODEL]: function ({commit}, para) {
      return api.datasource.discardTableModel(para.projectName, para.modelId)
    },
    [types.FETCH_NEWEST_TABLE_RANGE]: function ({commit}, para) {
      return api.datasource.fetchNewestTableRange(para.projectName, para.tableFullName)
    },
    [types.FETCH_BATCH_LOAD_TABLES]: function ({commit}, para) {
      return api.datasource.fetchBatchLoadTables(para.projectName)
    },
    [types.SAVE_TABLES_BATCH_LOAD]: function ({commit}, body) {
      return api.datasource.saveTablesBatchLoad(body)
    },
    [types.SAVE_SOURCE_CONFIG]: function ({commit}, body) {
      return api.datasource.saveSourceConfig(body)
    },
    // csv
    [types.VERIFY_CSV_CONN]: function ({commit}, data) {
      return api.datasource.verifyCsvConnection(data)
    },
    [types.GET_CSV_SAMPLE]: function ({commit}, data) {
      return api.datasource.getCsvSampleData(data)
    },
    [types.SAVE_CSV_INFO]: function ({commit}, para) {
      return api.datasource.saveCsvInfo(para.type, para.data)
    },
    [types.LOAD_CSV_SCHEME]: function ({commit}, data) {
      return api.datasource.loadCsvSchema(data)
    },
    [types.VERIFY_CSV_SQL]: function ({commit}, data) {
      return api.datasource.verifyCSVSql(data)
    },
    [types.FETCH_PARTITION_FORMAT]: function ({ commit }, data) {
      return api.datasource.fetchPartitionFormat(data)
    },
    [types.CHECK_SSB]: function ({ commit }) {
      return api.datasource.checkSSB()
    },
    [types.IMPORT_SSB_DATABASE]: function ({ commit }) {
      return api.datasource.importSSBDatabase()
    },
    [types.EXPORT_CSV]: function ({commit}, data) {
      return api.datasource.exportCSV(data)
    }
  }
}
