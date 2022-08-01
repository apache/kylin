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
