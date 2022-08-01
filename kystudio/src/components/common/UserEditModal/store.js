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
  editType: 'new',
  callback: null,
  totalGroups: [],
  form: {
    uuid: '',
    username: '',
    password: '',
    oldPassword: '',
    newPassword: '',
    disabled: false,
    admin: false,
    modeler: false,
    analyst: true,
    confirmPassword: '',
    authorities: ['ALL_USERS']
  },
  showCloseBtn: true,
  showCancelBtn: true
})

export default {
  // state深拷贝
  state: JSON.parse(initialState),
  mutations: {
    // 设置Modal中Form的field值
    [types.SET_MODAL_FORM]: (state, payload) => {
      state.form = {
        ...state.form,
        ...payload
      }

      if ('admin' in payload && payload['admin'] === true) {
        // 如果权限是admin，则加入ROLE_ADMIN
        state.form.authorities.push('ROLE_ADMIN')
      } else if ('admin' in payload && payload['admin'] === false) {
        // 如果权限不是admin，则剔除ROLE_ADMIN
        state.form.authorities = state.form.authorities.filter(authority => authority !== 'ROLE_ADMIN')
      }
    },
    // 显示Modal弹窗
    [types.SHOW_MODAL]: (state) => {
      state.isShow = true
    },
    // 隐藏Modal弹窗
    [types.HIDE_MODAL]: (state) => {
      state.isShow = false
    },
    // 还原Modal中Form的值为初始值
    [types.RESET_MODAL_FORM]: (state) => {
      state.form = JSON.parse(initialState).form
    },
    // 设置Modal中的值
    [types.SET_MODAL]: (state, payload) => {
      for (const key of Object.keys(state)) {
        switch (key) {
          // 有userDetail则设置modal from数据
          case 'form':
            payload.userDetail && setModalForm(payload, state)
            break
          // 有totalGroups则设置modal穿梭框数据
          case 'totalGroups':
            payload.totalGroups && setTotalGroups(payload, state)
            break
          // 设置modal的数据
          default: {
            key in payload && (state[key] = payload[key])
            break
          }
        }
      }
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { editType, userDetail, showCloseBtn = true, showCancelBtn = true }) {
      return new Promise(resolve => {
        commit(types.SET_MODAL, { editType, userDetail, showCloseBtn, showCancelBtn, callback: resolve })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

function setTotalGroups (payload, state) {
  const defaultGroup = {
    key: 'ALL_USERS',
    label: 'ALL_USERS',
    disabled: true
  }
  state.totalGroups = payload.totalGroups
    .filter(groupName => groupName !== defaultGroup.key)
    .map(value => {
      return {
        key: value,
        label: value,
        disabled: false
      }
    })
  // 将'ALL_USERS'排在第一
  state.totalGroups = [defaultGroup, ...state.totalGroups]
}

function setModalForm (payload, state) {
  const { authorities } = payload.userDetail
  // 将authorities中的authority铺平
  const userDetail = {
    ...payload.userDetail,
    authorities: authorities.map(item => item.authority)
  }
  state.form = {
    ...state.form,
    ...userDetail
  }
}

export { types }
