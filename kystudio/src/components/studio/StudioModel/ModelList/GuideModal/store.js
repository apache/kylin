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
  isShowDimAndMeasGuide: false,
  isShowBuildGuide: false,
  isStreamingModel: false,
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
      state.isShowDimAndMeasGuide = payload.isShowDimAndMeasGuide
      state.isShowBuildGuide = payload.isShowBuildGuide
      state.isStreamingModel = payload.isStreamingModel
    },
    [types.RESET_MODAL_FORM]: state => {
      state.form = JSON.parse(initialState).form
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { isShowDimAndMeasGuide, isShowBuildGuide, isStreamingModel }) {
      return new Promise(resolve => {
        commit(types.SET_MODAL_FORM, { isShowDimAndMeasGuide, isShowBuildGuide, isStreamingModel, callback: resolve })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}
export { types }
