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
import * as actionTypes from '../../../store/types'
import api from '../../../service/api'
import { handleSuccessAsync, handleError } from '../../../util'

export function getInitialState () {
  return {
    isShow: false,
    callback: null,
    project: null,
    type: 'one',
    models: [],
    form: {
      ids: [],
      exportOverProps: false,
      exportMultiplePartitionValues: false
    }
  }
}

function formatModelsStructure (response) {
  return response.map(model => ({
    ...model,
    id: model.uuid,
    name: model.name,
    nodeType: 'model',
    search: [model.name]
  }))
}

export default {
  state: getInitialState(),
  mutations: {
    [actionTypes.SHOW_MODAL] (state) {
      state.isShow = true
    },
    [actionTypes.HIDE_MODAL] (state) {
      state.isShow = false
    },
    [actionTypes.SET_MODAL] (state, payload) {
      for (const [key, value] of Object.entries(payload)) {
        state[key] = value
      }
    },
    [actionTypes.INIT_MODAL] (state) {
      for (const [key, value] of Object.entries(getInitialState())) {
        state[key] = value
      }
    },
    [actionTypes.SET_MODAL_FORM] (state, payload) {
      for (const [key, value] of Object.entries(payload)) {
        state.form[key] = value
      }
    },
    [actionTypes.RESET_MODAL_STATE] (state) {
      state.form = {ids: [], exportOverProps: false, exportMultiplePartitionValues: false}
    }
  },
  actions: {
    [actionTypes.CALL_MODAL] ({ commit }, payload) {
      return new Promise(resolve => {
        commit(actionTypes.INIT_MODAL)
        commit(actionTypes.SET_MODAL, { ...payload, callback: resolve })
        commit(actionTypes.SHOW_MODAL)
      })
    },
    [actionTypes.GET_MODELS_METADATA_STRUCTURE] ({ commit }, payload) {
      return new Promise(async (resolve, reject) => {
        try {
          const response = await api.model.getMetadataStructure(payload)
          const result = await handleSuccessAsync(response)
          const models = formatModelsStructure(result)

          commit(actionTypes.SET_MODAL, { models })
          resolve()
        } catch (e) {
          handleError(e)
          reject(e)
        }
      })
    }
  },
  namespaced: true
}
