export const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL',
  SET_ORIGIN_FORM: 'SET_ORIGIN_FORM'
}

const initialState = JSON.stringify({
  isShow: false,
  editType: 'add',
  callback: null,
  form: {
    key: '',
    value: ''
  },
  originForm: {
    key: '',
    value: ''
  }
})

export default {
  namespaced: true,
  state: JSON.parse(initialState),
  mutations: {
    [types.SET_MODAL_FORM] (state, payload) {
      state.form = {
        ...state.form,
        ...payload
      }
    },
    [types.SET_ORIGIN_FORM] (state, payload) {
      state.originForm = { ...payload }
    },
    [types.SET_MODAL] (state, payload) {
      for (const key of Object.keys(payload)) {
        if (key === 'form') {
          state.form.key = payload.form.key
          state.form.value = payload.form.value
        } else {
          state[key] = payload[key]
        }
      }
    },
    [types.SHOW_MODAL] (state) {
      state.isShow = true
    },
    [types.HIDE_MODAL] (state) {
      state.isShow = false
    },
    [types.RESET_MODAL_FORM] (state) {
      state.form = JSON.parse(initialState).form
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, payload) {
      commit(types.SET_MODAL, payload)
      commit(types.SET_ORIGIN_FORM, payload.form)
      commit(types.SHOW_MODAL)
    }
  }
}
