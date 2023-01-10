/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
  callback: null,
  form: {
    modelDesc: '',
    modelInstance: null,
    mode: '',
    isChangeModelLayout: false,
    exchangeJoinTableList: []
  }
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
      state.form.modelDesc = payload.modelDesc
      state.form.modelInstance = payload.modelInstance
      state.form.mode = payload.mode
      state.form.isChangeModelLayout = payload.isChangeModelLayout
      state.form.allDimension = payload.allDimension
      state.form.exchangeJoinTableList = payload.exchangeJoinTableList
      state.callback = payload.callback
    },
    // 还原Modal中的值为初始值
    [types.RESET_MODAL_FORM]: (state) => {
      state.form = JSON.parse(initialState).form
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { modelDesc, modelInstance, mode, allDimension, isChangeModelLayout, exchangeJoinTableList = [] }) {
      return new Promise(resolve => {
        commit(types.SET_MODAL, { modelDesc: modelDesc, modelInstance: modelInstance, mode: mode, allDimension: allDimension, isChangeModelLayout: isChangeModelLayout, exchangeJoinTableList, callback: resolve })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
