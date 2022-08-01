const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL',
  UPDATE_SYNC_NAME: 'UPDATE_SYNC_NAME'
}
// 声明：初始state状态
const initialState = JSON.stringify({
  isShow: false,
  modelDesc: [],
  selectedDimensions: [],
  callback: null,
  syncCommentToName: false
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
      state.modelDesc = payload.modelDesc
      state.modelInstance = payload.modelInstance
      state.callback = payload.callback
    },
    // 还原Modal中的值为初始值
    [types.RESET_MODAL_FORM]: (state) => {
      state.selectedDimensions = []
      state.modelDesc = null
      state.modelInstance = null
      state.syncCommentToName = false
    },
    [types.UPDATE_SYNC_NAME]: (state) => {
      state.syncCommentToName = !state.syncCommentToName
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { modelDesc, modelInstance }) {
      return new Promise(resolve => {
        commit(types.SET_MODAL, { modelDesc: modelDesc, modelInstance, callback: resolve })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
