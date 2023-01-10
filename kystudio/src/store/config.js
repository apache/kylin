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
import { cacheLocalStorage } from 'util'
import { getAvailableOptions } from '../util/specParser'
import spec from '../config/spec'
export default {
  state: {
    encodingTip: {
      dict: 'dicTip',
      fixed_length: 'fixedLengthTip',
      int: 'intTip',
      integer: 'integerTip',
      fixed_length_hex: 'fixedLengthHexTip',
      date: 'dataTip',
      time: 'timeTip',
      boolean: 'booleanTip',
      orderedbytes: 'orderedbytesTip'
    },
    defaultConfig: {
      cube: {},
      project: {}
    },
    layoutConfig: {
      briefMenu: localStorage.getItem('isBrief') === 'true',
      gloalProjectSelectShow: true,
      fullScreen: false
    },
    errorMsgBox: {
      isShow: false,
      msg: '',
      detail: ''
    },
    showLoadingBox: false,
    routerConfig: {
      currentPathName: ''
    },
    overLock: localStorage.getItem('buyit'),
    allOptionMaps: [],
    enableOptionMaps: {},
    disableOptionMaps: {},
    cachedHistory: ''
  },
  mutations: {
    [types.SAVE_DEFAULT_CONFIG]: function (state, { list, type }) {
      state.defaultConfig[type] = list
    },
    [types.TOGGLE_SCREEN]: function (state, isFull) {
      state.layoutConfig.fullScreen = isFull
    },
    [types.TOGGLE_MENU]: function (state, isBrief) {
      state.layoutConfig.briefMenu = isBrief
      cacheLocalStorage('isBrief', isBrief)
    },
    [types.INIT_SPEC]: function (state) {
      state.allOptionMaps = spec.allOptionMaps
      state.enableOptionMaps = spec.enableOptionMaps
      state.disableOptionMaps = spec.disableOptionMaps
    },
    [types.CACHE_HISTORY]: function (state, cachedHistory) {
      state.cachedHistory = cachedHistory
    }
  },
  actions: {
    [types.LOAD_DEFAULT_CONFIG]: function ({ commit }, type) {
      return api.config.getDefaults(type).then((response) => {
        commit(types.SAVE_DEFAULT_CONFIG, { type: type, list: response.data.data })
      })
    },
    [types.IS_CLOUD]: function ({ commit }, type) {
      return api.config.isCloud(type)
    }
  },
  getters: {
    briefMenuGet (state) {
      return state.layoutConfig.briefMenu
    },
    currentPathNameGet (state) {
      return state.routerConfig.currentPathName
    },
    isFullScreen (state) {
      return state.layoutConfig.fullScreen
    },
    availableMenus (state, getters, rootState, rootGetters) {
      if (rootState.route.name) {
        const groupRole = rootGetters.userAuthorities
        const projectRole = rootState.user.currentUserAccess
        const menu = rootState.route.name.toLowerCase()

        return getAvailableOptions('menu', { groupRole, projectRole, menu })
      } else {
        return []
      }
    },
    dashboardActions (state, getters, rootState, rootGetters) {
      const groupRole = rootGetters.userAuthorities
      const projectRole = rootState.user.currentUserAccess

      return getAvailableOptions('dashboardActions', { groupRole, projectRole })
    },
    systemActions (state, getters, rootState, rootGetters) {
      const groupRole = rootGetters.userAuthorities
      const projectRole = rootState.user.currentUserAccess

      return getAvailableOptions('systemActions', { groupRole, projectRole })
    }
  }
}
