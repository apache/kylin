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
export const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL',
  SET_ORIGIN_FORM: 'SET_ORIGIN_FORM'
}

const initialState = JSON.stringify({
  isShow: false,
  editType: 'add',
  callback: null,
  form: {
    key: '',
    value: ''
  },
  originForm: {
    key: '',
    value: ''
  }
})

export default {
  namespaced: true,
  state: JSON.parse(initialState),
  mutations: {
    [types.SET_MODAL_FORM] (state, payload) {
      state.form = {
        ...state.form,
        ...payload
      }
    },
    [types.SET_ORIGIN_FORM] (state, payload) {
      state.originForm = { ...payload }
    },
    [types.SET_MODAL] (state, payload) {
      for (const key of Object.keys(payload)) {
        if (key === 'form') {
          state.form.key = payload.form.key
          state.form.value = payload.form.value
        } else {
          state[key] = payload[key]
        }
      }
    },
    [types.SHOW_MODAL] (state) {
      state.isShow = true
    },
    [types.HIDE_MODAL] (state) {
      state.isShow = false
    },
    [types.RESET_MODAL_FORM] (state) {
      state.form = JSON.parse(initialState).form
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, payload) {
      commit(types.SET_MODAL, payload)
      commit(types.SET_ORIGIN_FORM, payload.form)
      commit(types.SHOW_MODAL)
    }
  }
}
