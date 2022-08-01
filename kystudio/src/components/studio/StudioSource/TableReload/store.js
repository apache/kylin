const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  CALL_MODAL: 'CALL_MODAL',
  RESET_MODAL: 'RESET_MODAL'
}

const initialState = JSON.stringify({
  isShow: false,
  checkData: null,
  tableName: '',
  callback: null
})

export default {

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
    // 还原Modal中Form的值为初始值
    [types.RESET_MODAL]: (state) => {
      const newState = JSON.parse(initialState)
      for (const key in state) {
        state[key] = newState[key]
      }
    },
    // 设置Modal中的值
    [types.SET_MODAL]: (state, payload) => {
      for (const key of Object.keys(state)) {
        switch (key) {
          // 设置modal的数据
          default: {
            payload[key] && (state[key] = payload[key])
            break
          }
        }
      }
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { checkData, tableName }) {
      return new Promise(resolve => {
        commit(types.SET_MODAL, { checkData, tableName, callback: resolve })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
