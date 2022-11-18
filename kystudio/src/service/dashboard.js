import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getModelStatistics: (para) => {
    return Vue.resource(apiUrl + 'dashboard/metric/model').get(para)
  },
  getQueryStatistics: (para) => {
    return Vue.resource(apiUrl + 'dashboard/metric/query').get(para)
  },
  // category: JOB, QUERY
  // dimension: QUERY:{count, avg_query_latency}
  // metric: filter by model or date(day, week)
  getChartData: (category, dimension, metric, projectName, modelName, startTime, endTime) => {
    return Vue.resource(apiUrl + `dashboard/chart/${category}/${dimension}/${metric}?projectName=${projectName}&modelName=${modelName}&startTime=${startTime}&endTime=${endTime}`).get()
  },
  getJobStatistics: (para) => {
    return Vue.resource(apiUrl + 'dashboard/metric/job').get(para)
  }
}
