const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  CALL_MODAL: 'CALL_MODAL',
  RESET_MODAL: 'RESET_MODAL'
}
const initialState = JSON.stringify({
  isShow: false,
  callback: null,
  cancelReject: null,
  customCallback: null,
  onlyCloseDialogReject: false, // 点击x关闭弹窗返reject
  msg: '',
  dangerouslyUseHTMLString: false,
  detailMsg: '',
  tableTitle: '',
  title: '',
  isBeta: false,
  dialogType: '',
  theme: '',
  details: [],
  detailTableData: [],
  detailColumns: [],
  isShowSelection: false,
  showDetailBtn: false, // 默认设为不显示详情按钮，如果默认显示，配置为不显示的弹窗，在关闭时会闪现详情按钮
  showCopyBtn: false,
  needCallbackWhenClose: false, // 数据源处的特殊需求，关闭时执行回调
  showDetailDirect: false,
  showIcon: true,
  customClass: '',
  showCopyTextLeftBtn: false,
  closeText: '',
  cancelText: '',
  submitText: '',
  isSubSubmit: false,
  isHideSubmit: false,
  submitSubText: '',
  needResolveCancel: false,
  needConcelReject: false,
  hideBottomLine: false,
  wid: '480px',
  isCenterBtn: false
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
    [types.SET_MODAL]: (state, payload = {}) => {
      // 为兼容以前没传参数，后来传参数，默认不可显示，会有闪现问题，所以默认隐藏，再通过参数判断是否传入，进行动态赋值
      // 如果不传 showDetailBtn 参数，就默认是显示的，如果传了，就取传的值
      payload['showDetailBtn'] = payload['showDetailBtn'] === undefined ? true : payload['showDetailBtn']
      payload['showDetailDirect'] = payload['showDetailDirect'] === undefined ? false : payload['showDetailDirect']
      for (const key in payload) {
        state[key] = payload[key]
      }
    },
    [types.RESET_MODAL]: (state) => {
      const newState = JSON.parse(initialState)
      for (const key in state) {
        if (newState[key]) {
          state[key] = newState[key]
        }
      }
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { dialogType = 'error', msg, dangerouslyUseHTMLString, tableTitle, detailMsg = '', title, isBeta = false, wid = '480px', details = [], detailTableData = [], detailColumns = [], isShowSelection = false, theme = 'plain', showDetailBtn = true, showIcon = true, showDetailDirect = false, customClass = '', showCopyTextLeftBtn = false, showCopyBtn = false, needCallbackWhenClose = false, customCallback = null, closeText = '', cancelText = '', submitText = '', isSubSubmit = false, isHideSubmit = false, submitSubText = '', isCenterBtn = false, needResolveCancel = false, needConcelReject = false, hideBottomLine = false, onlyCloseDialogReject = false }) {
      return new Promise(async (resolve, reject) => {
        commit(types.SET_MODAL, { dialogType, msg, dangerouslyUseHTMLString, tableTitle, detailMsg, title, isBeta, wid, details, detailTableData, detailColumns, isShowSelection, theme, showDetailBtn, showIcon, showDetailDirect, customClass, showCopyTextLeftBtn, showCopyBtn, needCallbackWhenClose, customCallback, closeText, cancelText, submitText, isSubSubmit, isHideSubmit, submitSubText, isCenterBtn, needResolveCancel, callback: resolve, needConcelReject, cancelReject: reject, hideBottomLine, onlyCloseDialogReject })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
