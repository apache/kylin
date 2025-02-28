const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL'
}
// 声明：初始state状态
const initialState = JSON.stringify({
  isShow: false,
  callback: null,
  form: {
    storage_type: 1, // 0 或 1 对应 v1 模型，3 对应 v3 模型
    uuid: '' // 模型 id
  }
})
export default {
  // state深拷贝
  state: JSON.parse(initialState),
  mutations: {
    // 显示Modal弹窗
    [types.SHOW_MODAL]: state => {
      state.isShow = true
    },
    // 隐藏Modal弹窗
    [types.HIDE_MODAL]: state => {
      state.isShow = false
    },
    [types.SET_MODAL_FORM]: (state, payload) => {
      state.callback = payload.callback
      state.form = {
        ...payload.form
      }
    },
    [types.RESET_MODAL_FORM]: state => {
      state.form = JSON.parse(initialState).form
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, data) {
      return new Promise(resolve => {
        // 统一 storage type 的值
        const temp = {
          ...data,
          storage_type: !data.storage_type || data.storage_type !== 3 ? 1 : data.storage_type
        }
        commit(types.SET_MODAL_FORM, { callback: resolve, form: temp })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}
export { types }
