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
import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getJobsList: (params) => {
    return Vue.resource(apiUrl + 'jobs{?job_names}').get(params)
  },
  getJobDetail: (para) => {
    return Vue.resource(apiUrl + 'jobs/' + para.job_id + '/detail').get(para)
  },
  losdWaittingJobModels: (para) => {
    return Vue.resource(apiUrl + 'jobs/waiting_jobs/models').get(para)
  },
  laodWaittingJobsByModel: (para) => {
    return Vue.resource(apiUrl + 'jobs/waiting_jobs').get(para)
  },
  getSlowQueries: (para) => {
    return Vue.resource(apiUrl + 'diag/slow_query').get(para.page)
  },
  getPushDownQueries: (para) => {
    return Vue.resource(apiUrl + 'diag/push_down').get(para.page)
  },
  exportPushDownQueries: (para) => {
    return Vue.resource(apiUrl + 'diag/export/push_down').save(para)
  },
  getStepOutputs: (para) => {
    return Vue.resource(apiUrl + 'jobs/' + para.jobId + '/steps/' + para.stepId + '/output?project=' + para.project).get()
  },
  resumeJob: (para) => {
    return Vue.resource(apiUrl + 'jobs/status').update(para)
  },
  restartJob: (para) => {
    return Vue.resource(apiUrl + 'jobs/status').update(para)
  },
  pauseJob: (para) => {
    return Vue.resource(apiUrl + 'jobs/status').update(para)
  },
  discardJob: (para) => {
    return Vue.resource(apiUrl + 'jobs/status').update(para)
  },
  removeJob: (para) => {
    return Vue.resource(apiUrl + 'jobs' + '{?job_ids}' + '{&project}').delete(para)
  },
  removeJobForAll: (para) => {
    return Vue.resource(apiUrl + 'jobs' + '{?job_ids}').delete(para)
  },
  loadDashboardJobInfo: (para) => {
    return Vue.resource(apiUrl + 'jobs/statistics').get(para)
  },
  loadJobChartData: (para) => {
    return Vue.resource(apiUrl + 'jobs/statistics/count').get(para)
  },
  loadJobBulidChartData: (para) => {
    return Vue.resource(apiUrl + 'jobs/statistics/duration_per_byte').get(para)
  },
  loadStreamingJobsList: (para) => {
    return Vue.resource(apiUrl + 'streaming_jobs{?job_types}{&model_names}').get(para)
  },
  getStreamingJobRecords: (para) => {
    return Vue.resource(apiUrl + 'streaming_jobs/records').get(para)
  },
  updateStreamingJobs: (para) => {
    return Vue.resource(apiUrl + 'streaming_jobs/status').update(para)
  },
  getStreamingChartData: (para) => {
    return Vue.resource(apiUrl + `streaming_jobs/stats/${para.job_id}`).get(para)
  },
  getModelObjectList: (para) => {
    return Vue.resource(apiUrl + 'streaming_jobs/model_name').get(para)
  },
  getJobSimpleLog: (para) => {
    return Vue.resource(apiUrl + `streaming_jobs/${para.job_id}/simple_log`).get({project: para.project})
  }
}
