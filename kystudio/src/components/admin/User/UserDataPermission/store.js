const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  CALL_MODAL: 'CALL_MODAL'
}
// 声明：初始state状态
const initialState = JSON.stringify({
  isShow: false,
  callback: null,
  dataPermission: false,
  userName: ''
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
    // 设置Modal中的值
    [types.SET_MODAL]: (state, payload) => {
      for (const key in payload) {
        state[key] = payload[key]
      }
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { dataPermission, userName }) {
      return new Promise(resolve => {
        commit(types.SET_MODAL, { dataPermission, userName, callback: resolve })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
