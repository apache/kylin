import { getGmtDateFromUtcLike } from '../../../util'

const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL',
  INIT_FORM: 'INIT_FORM'
}
// 声明：初始state状态
const initialState = JSON.stringify({
  isShow: false,
  editType: '',
  callback: null,
  format: 'yyyy-MM-dd',
  form: {
    isLoadExisted: false,
    loadDataRange: [new Date(), new Date()],
    freshDataRange: [new Date(), new Date()]
  },
  table: null,
  project: null,
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
    [types.SET_MODAL_FORM]: (state, payload = {}) => {
      state.form = { ...state.form, ...payload }
    },
    [types.SET_MODAL]: (state, payload = {}) => {
      for (const key in payload) {
        state[key] = payload[key]
      }
    },
    [types.INIT_FORM]: (state, payload = {}) => {
      const { table } = state
      state.form.isLoadExisted = table.isLoadExisted || false
      state.form.loadDataRange = [ getGmtDateFromUtcLike(table.endTime), null ]
      state.form.freshDataRange = [ getGmtDateFromUtcLike(table.startTime), getGmtDateFromUtcLike(table.endTime) ]
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { editType, project = {}, table = {}, model = {}, format }) {
      return new Promise((resolve, reject) => {
        try {
          commit(types.SET_MODAL, { editType, project, table, model, format, callback: resolve })
          commit(types.INIT_FORM)
          commit(types.SHOW_MODAL)
        } catch (e) {
          reject(e)
        }
      })
    }
  },
  namespaced: true
}

export { types }
