import * as actionTypes from '../../../../store/types'

export function getInitialState () {
  return {
    isShow: false,
    callback: null,
    sql: ''
  }
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
    }
  },
  actions: {
    [actionTypes.CALL_MODAL] ({ commit }, payload) {
      return new Promise(resolve => {
        commit(actionTypes.INIT_MODAL)
        commit(actionTypes.SET_MODAL, { ...payload, callback: resolve })
        commit(actionTypes.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}
