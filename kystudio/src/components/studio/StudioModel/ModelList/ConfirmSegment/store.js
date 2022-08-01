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
  title: '',
  subTitle: '',
  refrashWarningSegment: false,
  indexes: [],
  isRemoveIndex: false,
  submitText: '',
  model: null,
  isHybridBatch: false, // 标识是否是融合数据模型下的批数据模型
  callback: null
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
      state.title = payload.title
      state.subTitle = payload.subTitle
      state.refrashWarningSegment = payload.refrashWarningSegment
      state.indexes = payload.indexes
      state.isRemoveIndex = payload.isRemoveIndex
      state.submitText = payload.submitText
      state.isHybridBatch = payload.isHybridBatch || false
      state.model = payload.model
    },
    [types.RESET_MODAL_FORM]: state => {
      state.form = JSON.parse(initialState).form
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { title, subTitle, refrashWarningSegment, indexes, isRemoveIndex, submitText, isHybridBatch = false, model }) {
      return new Promise(resolve => {
        commit(types.SET_MODAL_FORM, { callback: resolve, title: title, subTitle: subTitle, refrashWarningSegment: refrashWarningSegment, indexes: indexes, isRemoveIndex: isRemoveIndex, submitText: submitText, isHybridBatch: isHybridBatch, model: model })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}
export { types }
