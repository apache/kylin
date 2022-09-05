import api from './../service/api'
import * as types from './types'

export default {
  state: {
    nodeList: [],
    maintenance_mode: false,
    latestUpdateTime: 0
  },
  mutations: {
    [types.SET_NODES_LIST] (state, data) {
      state.nodeList = data.servers
      state.maintenance_mode = data.status.maintenance_mode
    },
    'LATEST_UPDATE_TIME' (state) {
      state.latestUpdateTime = new Date().getTime()
    }
  },
  actions: {
    // 获取节点列表
    [types.GET_NODES_LIST] ({ commit, dispatch }, paras) {
      return new Promise((resolve, reject) => {
        api.system.loadOnlineNodes(paras).then(res => {
          const { data, code } = res.data
          if (code === '000') {
            commit(types.SET_NODES_LIST, data)
            commit('LATEST_UPDATE_TIME')
            resolve(data)
          } else {
            reject()
          }
        }).catch((e) => {
          reject(e)
        })
      })
    }
  },
  getters: {
    isOnlyQueryNode (state) {
      return state.nodeList.length && state.nodeList.filter(it => it.mode === 'query').length === state.nodeList.length
    },
    isOnlyJobNode (state) {
      return state.nodeList.length && state.nodeList.filter(it => it.mode === 'job').length === state.nodeList.length
    }
  }
}
