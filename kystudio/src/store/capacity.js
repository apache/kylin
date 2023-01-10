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
