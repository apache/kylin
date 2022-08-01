const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL'
}
// 声明：初始state状态
const initialState = JSON.stringify({
  isShow: false,
  isGenerateModel: false,
  callback: null,
  type: 'new',
  repairData: null
})

export default {
  // state深拷贝
  state: JSON.parse(initialState),
  mutations: {
    // 显示Modal弹窗
    [types.SHOW_MODAL]: (state) => {
      state.isShow = true
    },
    // 隐藏Modal弹窗
    [types.HIDE_MODAL]: (state) => {
      state.isShow = false
    },
    [types.SET_MODAL]: (state, payload) => {
      state.callback = payload.callback
      state.type = payload.type
      state.repairData = payload.data
    },
    // 还原Modal中的值为初始值
    [types.RESET_MODAL_FORM]: (state, payload) => {
      state.form = { ...state.form, ...payload }
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, {type, data}) {
      return new Promise(resolve => {
        commit(types.SET_MODAL, { callback: resolve, type: type || 'new', data: data || null })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
