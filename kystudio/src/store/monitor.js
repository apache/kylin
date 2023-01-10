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
import api from './../service/api'
import * as types from './types'
import { getAvailableOptions } from '../util/specParser'
export default {
  state: {
  },
  actions: {
    [types.LOAD_JOBS_LIST]: function ({ commit, state }, params) {
      return api.monitor.getJobsList(params)
    },
    [types.GET_JOB_DETAIL]: function ({ commit }, para) {
      return api.monitor.getJobDetail(para)
    },
    [types.LOAD_WAITTING_JOBS_BY_MODEL]: function ({ commit }, para) {
      return api.monitor.laodWaittingJobsByModel(para)
    },
    [types.EXPORT_PUSHDOWN]: function ({ commit }, para) {
      return api.monitor.exportPushDownQueries(para)
    },
    [types.LOAD_STEP_OUTPUTS]: function ({ commit }, para) {
      return api.monitor.getStepOutputs(para)
    },
    [types.RESUME_JOB]: function ({ commit }, para) {
      return api.monitor.resumeJob(para)
    },
    [types.RESTART_JOB]: function ({ commit }, para) {
      return api.monitor.restartJob(para)
    },
    [types.PAUSE_JOB]: function ({ commit }, para) {
      return api.monitor.pauseJob(para)
    },
    [types.DISCARD_JOB]: function ({ commit }, para) {
      return api.monitor.discardJob(para)
    },
    [types.REMOVE_JOB]: function ({ commit }, para) {
      return api.monitor.removeJob(para)
    },
    [types.ROMOVE_JOB_FOR_ALL]: function ({ commit }, para) {
      return api.monitor.removeJobForAll(para)
    },
    [types.LOAD_DASHBOARD_JOB_INFO]: function ({ commit }, para) {
      return api.monitor.loadDashboardJobInfo(para)
    },
    [types.LOAD_JOB_CHART_DATA]: function ({ commit }, para) {
      return api.monitor.loadJobChartData(para)
    },
    [types.LOAD_JOB_BULID_CHART_DATA]: function ({ commit }, para) {
      return api.monitor.loadJobBulidChartData(para)
    },
    [types.LOAD_STREAMING_JOBS_LIST]: function ({ commit }, para) {
      return api.monitor.loadStreamingJobsList(para)
    },
    [types.GET_STREAMING_JOB_RECORDS]: function ({ commit }, para) {
      return api.monitor.getStreamingJobRecords(para)
    },
    [types.UPDATE_STREAMING_JOBS]: function ({ commit }, para) {
      return api.monitor.updateStreamingJobs(para)
    },
    [types.GET_STREAMING_CHART_DATA]: function ({ commit }, para) {
      return api.monitor.getStreamingChartData(para)
    },
    [types.GET_MODEL_OBJECT_LIST]: function ({ commit }, para) {
      return api.monitor.getModelObjectList(para)
    },
    [types.GET_JOB_SIMPLE_LOG]: function ({ commit }, para) {
      return api.monitor.getJobSimpleLog(para)
    }
  },
  getters: {
    monitorActions (state, getters, rootState, rootGetters) {
      const groupRole = rootGetters.userAuthorities
      const projectRole = rootState.user.currentUserAccess

      return getAvailableOptions('monitorActions', { groupRole, projectRole })
    },
    insightActions (state, getters, rootState, rootGetters) {
      const groupRole = rootGetters.userAuthorities
      const projectRole = rootState.user.currentUserAccess

      return getAvailableOptions('insightActions', { groupRole, projectRole })
    },
    queryHistoryFilter (state, getters, rootState, rootGetters) {
      const groupRole = rootGetters.userAuthorities
      const projectRole = rootState.user.currentUserAccess

      return getAvailableOptions('queryHistoryFilter', { groupRole, projectRole })
    }
  }
}

