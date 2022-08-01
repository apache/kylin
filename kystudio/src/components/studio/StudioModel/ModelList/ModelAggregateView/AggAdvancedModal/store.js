const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL'
}
const initialState = JSON.stringify({
  isShow: false,
  callback: null,
  model: null
})

export default {
  state: JSON.parse(initialState),
  mutations: {
    [types.SHOW_MODAL]: (state) => {
      state.isShow = true
    },
    [types.HIDE_MODAL]: (state) => {
      state.isShow = false
    },
    [types.SET_MODAL_FORM]: (state, payload) => {
      state.model = payload.model
      state.callback = payload.callback
    },
    [types.RESET_MODAL_FORM]: (state) => {
      state.form = JSON.parse(initialState).form
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, {model}) {
      return new Promise(resolve => {
        commit(types.SET_MODAL_FORM, {model, callback: resolve})
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
