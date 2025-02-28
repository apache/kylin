import api from './../service/api'
import * as types from './types'
export default {
  state: {
    internalTableList: [],
    internalTableListTotal: 0,
    internalTableListPageOffset: 0,
    internalTableListPageLimit: 20
  },
  mutations: {
    [types.MUTATE_REPLACE_INTERNAL_TABLE_LIST]: function (state, result) {
      state.internalTableList = result.value
      state.internalTableListTotal = result.total_size
      state.internalTableListPageOffset = result.offset
      state.internalTableListPageLimit = result.limit
    }
  },
  actions: {
    [types.GET_INTERNAL_TABLES]: function ({ commit }, params) {
      return new Promise((resolve, reject) => {
        api.internalTable.getInternalTableList(params).then((response) => {
          commit(types.MUTATE_REPLACE_INTERNAL_TABLE_LIST, response.data.data)
          resolve(response)
        }, (res) => {
          commit(types.MUTATE_REPLACE_INTERNAL_TABLE_LIST, { value: [], total_size: 0, offset: 0, limit: 20 })
          reject(res)
        })
      })
    },
    [types.UPDATE_INTERNAL_TABLE]: function ({ commit }, params) {
      return api.internalTable.updateInternalTable(params)
    },
    [types.CREATE_INTERNAL_TABLE]: function ({ commit }, params) {
      return api.internalTable.createInternalTable(params)
    },
    [types.GET_INTERNAL_TABLE_DETAILS]: function ({ commit }, params) {
      return api.internalTable.getInternalTableDetails(params)
    },
    [types.DELETE_INTERNAL_TABLE]: function ({ commit }, params) {
      return api.internalTable.deleteInternalTable(params)
    },
    [types.LOAD_SINGLE_INTERNAL_TABLE]: function ({ commit }, params) {
      return api.internalTable.loadSingleInternalTable(params)
    },
    [types.BATCH_LOAD_INTERNAL_TABLES]: function ({ commit }, params) {
      return api.internalTable.batchLoadInternalTables(params)
    },
    [types.DELETE_INTERNAL_TABLE_PARTITIONS]: function ({ commit }, params) {
      return api.internalTable.deletePartitions(params)
    },
    [types.DELETE_ALL_DATA_INTERNAL_TABLE]: function ({ commit }, params) {
      return api.internalTable.deleteAllData(params)
    }
  },
  getters: {
  }
}
